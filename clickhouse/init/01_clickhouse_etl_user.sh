#!/bin/bash
# Creates the least-privilege user the ClickHouse Kafka Connect sink
# connector actually connects as. CLICKHOUSE_USER/CLICKHOUSE_PASSWORD (with
# CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 in docker-compose.yml) is a
# bootstrap/admin identity used only to run this script -- nothing else
# should authenticate as it.
#
# Unlike PeerDB's ClickHouse peer, this connector never runs CREATE TABLE or
# ALTER TABLE -- destination tables are pre-created by hand in
# 02_destination_tables.sql and schema drift is NOT propagated
# automatically (see docs/architecture.md). So the grant set here is
# deliberately narrower than PeerDB's: just INSERT/SELECT, scoped to the
# debezium_cdc database. Adjusted if empirical testing (Stage 3/4) surfaces
# a real requirement beyond this.
set -e

CH=(clickhouse-client -u "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}")

"${CH[@]}" -q "CREATE USER IF NOT EXISTS ${CLICKHOUSE_ETL_USER} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_ETL_PASSWORD}'"
"${CH[@]}" -q "GRANT INSERT, SELECT ON ${CLICKHOUSE_DB}.* TO ${CLICKHOUSE_ETL_USER}"

echo "$0: clickhouse_etl user provisioned with least-privilege grants"
