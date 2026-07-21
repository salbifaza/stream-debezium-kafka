#!/usr/bin/env bash
# A scriptable status check across the whole pipeline -- the analog to
# stream-cdc-peerdb's mirror_status.sh, but there's no single "catalog" db
# to query here: connector state lives in Kafka Connect's REST API,
# consumer lag lives in Kafka's own consumer-group offsets, and
# replication-slot health lives on the source Postgres directly, because
# that's genuinely three different systems instead of PeerDB's one.
set -euo pipefail
cd "$(dirname "$0")/.."

CONNECT_URL="${CONNECT_URL:-http://localhost:8087}"
COMPOSE="docker compose"

echo "== Connector + task status (Kafka Connect REST API) =="
for name in ecommerce-pg-source ecommerce-ch-sink; do
    curl -sf "${CONNECT_URL}/connectors/${name}/status" | jq .
done

echo
echo "== Consumer lag: ecommerce-ch-sink's consumer group (per topic-partition) =="
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group connect-ecommerce-ch-sink 2>&1 || echo "  (consumer group not found yet -- sink connector may not have started consuming)"

echo
echo "== Replication slot size on source (grows if the pipeline falls behind or stalls) =="
$COMPOSE exec -T source-postgres psql -U "${SOURCE_PG_USER:-ecommerce}" -d "${SOURCE_PG_DB:-ecommerce}" -c "
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;"

echo
echo "== Per-table row counts in ClickHouse (current state, is_deleted excluded) =="
$COMPOSE exec -T clickhouse clickhouse-client --user "${CLICKHOUSE_USER:-ch_admin}" --password "${CLICKHOUSE_PASSWORD:-ch_admin_password}" -q "
SELECT 'categories' AS t, count() FROM debezium_cdc.categories FINAL WHERE is_deleted = 0
UNION ALL SELECT 'customers', count() FROM debezium_cdc.customers FINAL WHERE is_deleted = 0
UNION ALL SELECT 'products', count() FROM debezium_cdc.products FINAL WHERE is_deleted = 0
UNION ALL SELECT 'orders', count() FROM debezium_cdc.orders FINAL WHERE is_deleted = 0
UNION ALL SELECT 'order_items', count() FROM debezium_cdc.order_items FINAL WHERE is_deleted = 0
UNION ALL SELECT 'payments', count() FROM debezium_cdc.payments FINAL WHERE is_deleted = 0
FORMAT PrettyCompact;"
