# Extends Debezium's own Kafka Connect image (already ships the Debezium
# Postgres connector under $KAFKA_CONNECT_PLUGINS_DIR) with the official
# ClickHouse Kafka Connect sink connector added as a second, independent
# plugin -- this Dockerfile IS the "four extra systems to assemble"
# tradeoff stream-cdc-peerdb's README names: PeerDB ships this pairing as
# one product, here it's a base image plus one more connector bolted on by
# hand.
FROM quay.io/debezium/connect:3.6.0

ARG CLICKHOUSE_KAFKA_CONNECT_VERSION=1.4.0

# /kafka/connect (= $KAFKA_CONNECT_PLUGINS_DIR) is world-writable in the
# base image specifically so derived images can add plugins without
# switching to root -- runs as the same non-root `kafka` user the base
# image already uses.
RUN curl -fsSL \
      "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/v${CLICKHOUSE_KAFKA_CONNECT_VERSION}/clickhouse-kafka-connect-v${CLICKHOUSE_KAFKA_CONNECT_VERSION}.zip" \
      -o /tmp/clickhouse-kafka-connect.zip \
    && mkdir -p "${KAFKA_CONNECT_PLUGINS_DIR}/clickhouse-kafka-connect" \
    && unzip -q /tmp/clickhouse-kafka-connect.zip -d "${KAFKA_CONNECT_PLUGINS_DIR}/clickhouse-kafka-connect" \
    && rm /tmp/clickhouse-kafka-connect.zip
