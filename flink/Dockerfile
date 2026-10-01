# Flink with the Kafka SQL connector added -- the stream processor that
# turns Debezium's per-table change topics into gold aggregates. Flink's
# own distribution already ships flink-json (which contains the
# `debezium-json` format) under /opt/flink/lib; the Kafka connector is
# released separately from Flink itself, so it's the one jar added here.
# Connector version suffix (-2.2) must match the Flink minor version.
FROM flink:2.2.1-scala_2.12-java17

ARG FLINK_KAFKA_CONNECTOR_VERSION=5.0.0-2.2

ADD --chown=flink:flink \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
    /opt/flink/lib/

# Checkpoint directory, shared by jobmanager + taskmanager through a named
# volume. Created here so the volume inherits flink-user ownership.
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
