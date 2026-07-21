#!/usr/bin/env bash
# Verifies the Debezium source -> Kafka -> ClickHouse sink pipeline:
# (1) row counts match after the initial snapshot, (2) a live
# insert/update/delete against source-postgres shows up in ClickHouse
# within a timeout. Same shape/output style as stream-cdc-peerdb's
# verify_cdc.sh so the two are directly comparable. Run after
# scripts/register_connectors.sh and once both connectors report RUNNING.
set -euo pipefail

COMPOSE="docker compose"
PG_EXEC="$COMPOSE exec -T source-postgres psql -U ${SOURCE_PG_USER:-ecommerce} -d ${SOURCE_PG_DB:-ecommerce} -tA"
CH_EXEC="$COMPOSE exec -T clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-ch_admin} --password ${CLICKHOUSE_PASSWORD:-ch_admin_password}"

TABLES=(categories customers products orders order_items payments)

echo "== Step 1: row counts, source vs. ClickHouse (FINAL, excluding soft-deletes) =="
fail=0
for t in "${TABLES[@]}"; do
    src_count=$($PG_EXEC -c "SELECT count(*) FROM ${t};")
    ch_count=$($CH_EXEC -q "SELECT count() FROM debezium_cdc.${t} FINAL WHERE is_deleted = 0;")
    status="OK"
    if [ "$src_count" != "$ch_count" ]; then
        status="MISMATCH"
        fail=1
    fi
    printf "  %-14s source=%-6s clickhouse=%-6s %s\n" "$t" "$src_count" "$ch_count" "$status"
done
if [ "$fail" -ne 0 ]; then
    echo "Row count mismatch -- either the initial snapshot hasn't finished or the pipeline has fallen behind. Aborting." >&2
    exit 1
fi

echo
echo "== Step 2: live CDC test (insert / update / delete) =="
marker="cdc-verify-$(date +%s)"
echo "  inserting marker category '${marker}'..."
$PG_EXEC -c "INSERT INTO categories (name) VALUES ('${marker}');" >/dev/null

echo "  updating order_id=1 status to 'cancelled' (was 'delivered' in seed data)..."
$PG_EXEC -c "UPDATE orders SET status = 'cancelled', updated_at = now() WHERE order_id = 1;" >/dev/null

del_id=$($PG_EXEC -c "SELECT order_item_id FROM order_items ORDER BY order_item_id DESC LIMIT 1;")
echo "  deleting order_items.order_item_id=${del_id}..."
$PG_EXEC -c "DELETE FROM order_items WHERE order_item_id = ${del_id};" >/dev/null

echo "  polling ClickHouse for propagation (up to 60s)..."
deadline=$((SECONDS + 60))
insert_ok=0; update_ok=0; delete_ok=0
while [ $SECONDS -lt $deadline ]; do
    [ "$insert_ok" -eq 0 ] && ch_val=$($CH_EXEC -q "SELECT count() FROM debezium_cdc.categories FINAL WHERE name = '${marker}' AND is_deleted = 0;") && [ "$ch_val" = "1" ] && insert_ok=1
    [ "$update_ok" -eq 0 ] && ch_val=$($CH_EXEC -q "SELECT status FROM debezium_cdc.orders FINAL WHERE order_id = 1;") && [ "$ch_val" = "cancelled" ] && update_ok=1
    [ "$delete_ok" -eq 0 ] && ch_val=$($CH_EXEC -q "SELECT is_deleted FROM debezium_cdc.order_items FINAL WHERE order_item_id = ${del_id};") && [ "$ch_val" = "1" ] && delete_ok=1
    if [ "$insert_ok" -eq 1 ] && [ "$update_ok" -eq 1 ] && [ "$delete_ok" -eq 1 ]; then
        break
    fi
    sleep 2
done

printf "  insert propagated: %s\n" "$([ "$insert_ok" -eq 1 ] && echo yes || echo NO)"
printf "  update propagated: %s\n" "$([ "$update_ok" -eq 1 ] && echo yes || echo NO)"
printf "  delete propagated (soft-delete tombstone): %s\n" "$([ "$delete_ok" -eq 1 ] && echo yes || echo NO)"

if [ "$insert_ok" -eq 1 ] && [ "$update_ok" -eq 1 ] && [ "$delete_ok" -eq 1 ]; then
    echo
    echo "CDC verification PASSED."
    exit 0
else
    echo
    echo "CDC verification FAILED -- one or more changes did not propagate within 60s." >&2
    exit 1
fi
