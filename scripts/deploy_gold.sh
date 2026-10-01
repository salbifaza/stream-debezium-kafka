#!/usr/bin/env bash
# Deploys the streaming gold layer, in dependency order:
#   1. create the gold.* Kafka topics Flink writes to,
#   2. create ClickHouse's gold tables + Kafka engine consumers
#      (clickhouse/gold.sql) -- these subscribe on creation, hence after 1,
#   3. submit the Flink SQL job (flink/sql/gold.sql) unless it's already
#      running.
# Idempotent, same as register_connectors.sh: every step is a no-op if
# already done. Run after register_connectors.sh, so the Debezium topics
# Flink reads from exist.
#
# If the Flink cluster restarts, the job is gone (no HA here) and this
# script resubmits it. It then re-reads every source topic from the
# beginning and re-emits the whole gold history; ClickHouse converges on
# the same final state because each re-emitted message has a newer
# offset, i.e. a newer `_version`.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; . ./.env; set +a
fi

FLINK_URL="${FLINK_URL:-http://localhost:8088}"
JOB_NAME="ecommerce-gold"
GOLD_TOPICS=(gold.daily_revenue gold.category_revenue gold.customer_ltv)
COMPOSE="docker compose"

echo "Waiting for Flink REST API at ${FLINK_URL} ..."
deadline=$((SECONDS + 90))
until curl -sf "${FLINK_URL}/overview" >/dev/null 2>&1; do
    if [ $SECONDS -ge $deadline ]; then
        echo "Flink did not become reachable within 90s." >&2
        exit 1
    fi
    sleep 2
done

echo "== Creating gold topics =="
for topic in "${GOLD_TOPICS[@]}"; do
    $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$topic" \
        --partitions 1 --replication-factor 1
done

echo "== Applying clickhouse/gold.sql =="
$COMPOSE exec -T clickhouse clickhouse-client \
    --user "${CLICKHOUSE_USER:-ch_admin}" --password "${CLICKHOUSE_PASSWORD:-ch_admin_password}" \
    --multiquery < clickhouse/gold.sql

job_state() {
    curl -sf "${FLINK_URL}/jobs/overview" \
        | jq -r --arg n "$JOB_NAME" \
            '[.jobs[] | select(.name == $n and (.state | IN("RUNNING","CREATED","INITIALIZING","RESTARTING")))][0].state // empty'
}

if [ -n "$(job_state)" ]; then
    echo "== Flink job '${JOB_NAME}' already $(job_state); not resubmitting =="
else
    echo "== Submitting Flink job '${JOB_NAME}' (flink/sql/gold.sql) =="
    # sql-client echoes the whole script back; show only the job ID, or
    # everything if it reported an error (its exit code isn't reliable).
    out=$($COMPOSE exec -T flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/gold.sql </dev/null 2>&1) || true
    if grep -q '\[ERROR\]' <<<"$out"; then
        echo "$out" >&2
        echo "Flink SQL submission failed (see above)." >&2
        exit 1
    fi
    grep -E '^Job ID:' <<<"$out" | sed 's/^/  /'
fi

echo "Waiting for job to reach RUNNING ..."
deadline=$((SECONDS + 60))
until [ "$(job_state)" = "RUNNING" ]; do
    if [ $SECONDS -ge $deadline ]; then
        echo "Flink job did not reach RUNNING within 60s (state: '$(job_state)')." >&2
        exit 1
    fi
    sleep 2
done
echo "Flink job '${JOB_NAME}' is RUNNING. UI: ${FLINK_URL}"
