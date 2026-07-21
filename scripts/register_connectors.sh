#!/usr/bin/env bash
# Registers both connectors against Kafka Connect's REST API. Idempotent:
# PUT /connectors/<name>/config creates the connector if it doesn't exist,
# or updates it in place (triggering a restart) if it does -- same
# re-runnable-script discipline as stream-cdc-peerdb's create_mirror.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

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
    curl -sf -X PUT \
        -H "Content-Type: application/json" \
        -d "$(jq -c '.config' "$file")" \
        "${CONNECT_URL}/connectors/${name}/config" | jq .
    echo
}

register connectors/pg-source-connector.json
register connectors/ch-sink-connector.json

echo "== Connector status =="
for name in ecommerce-pg-source ecommerce-ch-sink; do
    curl -sf "${CONNECT_URL}/connectors/${name}/status" | jq .
done
