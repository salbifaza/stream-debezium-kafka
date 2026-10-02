#!/bin/bash
# Creates the least-privilege user the ClickHouse Kafka Connect sink
# connector actually connects as. CLICKHOUSE_USER/CLICKHOUSE_PASSWORD (with
# CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 in docker-compose.yml) is a
# bootstrap/admin identity used only to run this script -- nothing else
# should authenticate as it.
#
# Unlike PeerDB's ClickHouse peer, this connector never runs CREATE TABLE --
# destination tables are generated from the Postgres catalog and created,
# as the admin user, by scripts/create_ch_tables.sh.
# It does run ALTER TABLE ... ADD COLUMN, because the sink runs with
# `auto.evolve=true` (connectors/ch-sink-connector.json): a column added on
# the source is added in ClickHouse automatically. Without this grant the
# task fails with ACCESS_DENIED on the first new column (tested -- see
# docs/architecture.md). Still narrower than PeerDB's grant set: add-column
# only (no drop/modify/rename), scoped to the debezium_cdc database.
set -e

CH=(clickhouse-client -u "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}")

"${CH[@]}" -q "CREATE USER IF NOT EXISTS ${CLICKHOUSE_ETL_USER} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_ETL_PASSWORD}'"
"${CH[@]}" -q "GRANT INSERT, SELECT, ALTER ADD COLUMN ON ${CLICKHOUSE_DB}.* TO ${CLICKHOUSE_ETL_USER}"

echo "$0: clickhouse_etl user provisioned with least-privilege grants"
