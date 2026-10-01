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

if [ "$insert_ok" -ne 1 ] || [ "$update_ok" -ne 1 ] || [ "$delete_ok" -ne 1 ]; then
    echo
    echo "CDC verification FAILED -- one or more changes did not propagate within 60s." >&2
    exit 1
fi

echo
echo "== Step 3: gold layer (Flink) matches the same aggregates computed in Postgres =="
# The source of truth for a gold table is the same query run directly
# against Postgres. Step 2's changes already exercise retraction (order 1
# cancelled -> leaves daily/category/customer totals) and deletes (an
# order item removed); the country toggle below adds a dimension change,
# which has to update an already-joined gold row in place.
echo "  toggling customers.customer_id=4 country (US <-> CA)..."
$PG_EXEC -c "UPDATE customers SET country = CASE country WHEN 'US' THEN 'CA' ELSE 'US' END, updated_at = now() WHERE customer_id = 4;" >/dev/null
changed_at=$SECONDS

# Each pair renders the same rows as identical 'a|b|c' lines, in the same order.
PG_DAILY="SELECT d || '|' || n || '|' || r FROM (
    SELECT (created_at AT TIME ZONE 'UTC')::date::text AS d, count(*) AS n, sum(order_total_cents) AS r
    FROM orders WHERE status <> 'cancelled' GROUP BY 1) x ORDER BY d;"
CH_DAILY="SELECT concat(toString(order_date), '|', toString(orders_count), '|', toString(revenue_cents))
    FROM gold.daily_revenue FINAL ORDER BY order_date;"

PG_CATEGORY="SELECT c.category_id || '|' || c.name || '|' || s.units || '|' || s.rev FROM (
    SELECT p.category_id, sum(oi.quantity) AS units, sum(oi.quantity * oi.unit_price_cents) AS rev
    FROM order_items oi JOIN orders o USING (order_id) JOIN products p USING (product_id)
    WHERE o.status <> 'cancelled' GROUP BY p.category_id) s
    JOIN categories c USING (category_id) ORDER BY c.category_id;"
CH_CATEGORY="SELECT concat(toString(category_id), '|', category_name, '|', toString(units_sold), '|', toString(revenue_cents))
    FROM gold.category_revenue FINAL ORDER BY category_id;"

PG_LTV="SELECT cu.customer_id || '|' || cu.email || '|' || cu.country || '|' || coalesce(a.n, 0) || '|' || coalesce(a.v, 0)
    FROM customers cu LEFT JOIN (
        SELECT customer_id, count(*) AS n, sum(order_total_cents) AS v
        FROM orders WHERE status <> 'cancelled' GROUP BY customer_id) a USING (customer_id)
    ORDER BY cu.customer_id;"
CH_LTV="SELECT concat(toString(customer_id), '|', email, '|', country, '|', toString(orders_count), '|', toString(lifetime_value_cents))
    FROM gold.customer_ltv FINAL ORDER BY customer_id;"

GOLD_TABLES=(daily_revenue category_revenue customer_ltv)
declare -A PG_Q=([daily_revenue]="$PG_DAILY" [category_revenue]="$PG_CATEGORY" [customer_ltv]="$PG_LTV")
declare -A CH_Q=([daily_revenue]="$CH_DAILY" [category_revenue]="$CH_CATEGORY" [customer_ltv]="$CH_LTV")
declare -A gold_ok=([daily_revenue]=0 [category_revenue]=0 [customer_ltv]=0)

echo "  polling gold tables until they match Postgres (up to 60s)..."
deadline=$((SECONDS + 60))
while [ $SECONDS -lt $deadline ]; do
    all_ok=1
    for g in "${GOLD_TABLES[@]}"; do
        if [ "${gold_ok[$g]}" -eq 0 ]; then
            if [ "$($PG_EXEC -c "${PG_Q[$g]}")" = "$($CH_EXEC -q "${CH_Q[$g]}" 2>/dev/null)" ]; then
                gold_ok[$g]=1
            else
                all_ok=0
            fi
        fi
    done
    [ "$all_ok" -eq 1 ] && break
    sleep 1
done
elapsed=$((SECONDS - changed_at))

gold_fail=0
for g in "${GOLD_TABLES[@]}"; do
    rows=$($PG_EXEC -c "${PG_Q[$g]}" | wc -l)
    if [ "${gold_ok[$g]}" -eq 1 ]; then
        printf "  gold.%-17s %3s rows, identical to Postgres  OK\n" "$g" "$rows"
    else
        printf "  gold.%-17s MISMATCH -- diff (< postgres, > clickhouse):\n" "$g"
        diff <($PG_EXEC -c "${PG_Q[$g]}") <($CH_EXEC -q "${CH_Q[$g]}" 2>&1) | sed 's/^/      /' || true
        gold_fail=1
    fi
done

if [ "$gold_fail" -ne 0 ]; then
    echo
    echo "CDC verification FAILED -- gold layer did not converge within 60s. Check 'make status' and the Flink UI (http://localhost:8088)." >&2
    exit 1
fi

echo "  gold converged within ~${elapsed}s of the last source change."
echo
echo "CDC verification PASSED."
