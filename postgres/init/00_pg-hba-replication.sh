#!/bin/sh
# Debezium's Kafka Connect container connects from another container on the
# compose network, not localhost, so the default pg_hba.conf (which only
# trusts local replication connections) won't let it open a replication
# stream. Add an explicit, password-authenticated rule scoped to the
# replication pseudo-database rather than reaching for `trust`.
set -e
echo "host replication ${POSTGRES_USER} 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
