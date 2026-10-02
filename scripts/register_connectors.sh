#!/usr/bin/env bash
# Registers both connectors against Kafka Connect's REST API. Idempotent:
# PUT /connectors/<name>/config creates the connector if it doesn't exist,
# or updates it in place (triggering a restart) if it does -- same
# re-runnable-script discipline as stream-cdc-peerdb's create_mirror.sh.
#
# Credentials from .env are injected at registration time so the checked-in
# JSON files don't need to match .env exactly -- change .env, re-run this
# script, and the connector picks up the new values.
set -euo pipefail
cd "$(dirname "$0")/.."

# Source .env if it exists (for credential substitution)
if [ -f .env ]; then
    set -a; . ./.env; set +a
fi

CONNECT_URL="${CONNECT_URL:-http://localhost:8087}"

echo "Waiting for Kafka Connect REST API at ${CONNECT_URL} ..."
deadline=$((SECONDS + 90))
until curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
    if [ $SECONDS -ge $deadline ]; then
        echo "Kafka Connect did not become reachable within 90s." >&2
        exit 1
    fi
    sleep 2
done

register() {
    local file="$1"
    local name
    name=$(jq -r '.name' "$file")
    echo "== Registering ${name} (from ${file}) =="

    # Inject credentials from env into the connector config at registration
    # time. This keeps the checked-in JSON files credential-free defaults
    # while allowing .env overrides to take effect without editing JSON.
    local config
    config=$(jq -c '.config' "$file")
    config=$(echo "$config" | jq -c \
        --arg pg_user "${SOURCE_PG_USER:-ecommerce}" \
        --arg pg_pass "${SOURCE_PG_PASSWORD:-ecommerce}" \
        --arg ch_user "${CLICKHOUSE_ETL_USER:-clickhouse_etl}" \
        --arg ch_pass "${CLICKHOUSE_ETL_PASSWORD:-clickhouse_etl_password}" \
        'if .["connector.class"] | test("Postgres") then
            .["database.user"] = $pg_user | .["database.password"] = $pg_pass
         else
            .username = $ch_user | .password = $ch_pass
         end')

    curl -sf -X PUT \
        -H "Content-Type: application/json" \
        -d "$config" \
        "${CONNECT_URL}/connectors/${name}/config" | jq .
    echo
}

register connectors/pg-source-connector.json
register connectors/ch-sink-connector.json

echo "== Connector status =="
# A just-created connector's status 404s until Connect has written it.
for name in ecommerce-pg-source ecommerce-ch-sink; do
    deadline=$((SECONDS + 30))
    until status=$(curl -sf "${CONNECT_URL}/connectors/${name}/status"); do
        if [ $SECONDS -ge $deadline ]; then
            echo "No status for ${name} within 30s." >&2
            exit 1
        fi
        sleep 1
    done
    jq . <<<"$status"
done
