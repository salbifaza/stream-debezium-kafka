-- Gold tables in ClickHouse, fed from the gold.* changelog topics Flink
-- writes (flink/sql/gold.sql). Applied by scripts/deploy_gold.sh, not by
-- the container's init directory: the Kafka engine tables below subscribe
-- to their topics on creation, so the topics must already exist, and
-- they don't until deploy_gold.sh creates them.
--
-- Ingestion is ClickHouse's own Kafka table engine rather than a second
-- Kafka Connect sink connector. The sink connector's Debezium mode only
-- understands envelopes produced by a Debezium *source* connector (it
-- reads `source.lsn` for the version), which Flink's output doesn't
-- carry; the Kafka engine exposes the message's `_offset`, which is a
-- better version anyway (see below).
--
-- Per gold table, three objects:
--   <name>_queue  Kafka engine table: a consumer, not storage. Each
--                 message is Flink's debezium-json: {"op","before","after"}.
--   <name>_mv     moves each message into the real table, picking the
--                 row from `after` (op c) or `before` (op d).
--   <name>        ReplacingMergeTree(_version, is_deleted). `_version` is
--                 the Kafka offset: Flink partitions by the gold row's
--                 key, so all changes to one row are in one partition,
--                 where offsets are strictly increasing. `is_deleted`
--                 is the engine's own delete marker, so `FINAL` alone
--                 hides retracted rows -- read as `SELECT ... FINAL`,
--                 no `WHERE is_deleted = 0` needed (unlike debezium_cdc).
--
-- An updated gold row arrives as two messages, a retraction (op d) then
-- the new value (op c) at a higher offset, so the new value wins.

CREATE DATABASE IF NOT EXISTS gold;

-- ── gold.daily_revenue ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    order_date    Date,
    orders_count  UInt64,
    revenue_cents Int64,
    _version      UInt64,
    is_deleted    UInt8
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY order_date;

CREATE TABLE IF NOT EXISTS gold.daily_revenue_queue (
    op     String,
    before Nullable(String),
    after  Nullable(String)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'dbz-kafka:9092',
         kafka_topic_list = 'gold.daily_revenue',
         kafka_group_name = 'clickhouse-gold-daily_revenue',
         kafka_format = 'JSONEachRow',
         -- Flush consumed messages to the MV every 1s instead of the
         -- default 7.5s -- the dominant term in gold end-to-end latency.
         kafka_flush_interval_ms = 1000,
         -- Keep nested JSON objects (before/after) as raw strings.
         input_format_json_read_objects_as_strings = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.daily_revenue_mv TO gold.daily_revenue AS
WITH ifNull(after, before) AS r
SELECT
    toDate(JSONExtractString(r, 'order_date')) AS order_date,
    JSONExtractUInt(r, 'orders_count')         AS orders_count,
    JSONExtractInt(r, 'revenue_cents')         AS revenue_cents,
    _offset                                    AS _version,
    op = 'd'                                   AS is_deleted
FROM gold.daily_revenue_queue;

-- ── gold.category_revenue ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.category_revenue (
    category_id   Int32,
    category_name String,
    units_sold    Int64,
    revenue_cents Int64,
    _version      UInt64,
    is_deleted    UInt8
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS gold.category_revenue_queue (
    op     String,
    before Nullable(String),
    after  Nullable(String)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'dbz-kafka:9092',
         kafka_topic_list = 'gold.category_revenue',
         kafka_group_name = 'clickhouse-gold-category_revenue',
         kafka_format = 'JSONEachRow',
         kafka_flush_interval_ms = 1000,
         input_format_json_read_objects_as_strings = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.category_revenue_mv TO gold.category_revenue AS
WITH ifNull(after, before) AS r
SELECT
    toInt32(JSONExtractInt(r, 'category_id')) AS category_id,
    JSONExtractString(r, 'category_name')     AS category_name,
    JSONExtractInt(r, 'units_sold')           AS units_sold,
    JSONExtractInt(r, 'revenue_cents')        AS revenue_cents,
    _offset                                   AS _version,
    op = 'd'                                  AS is_deleted
FROM gold.category_revenue_queue;

-- ── gold.customer_ltv ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.customer_ltv (
    customer_id          Int32,
    email                String,
    country              String,
    orders_count         UInt64,
    lifetime_value_cents Int64,
    _version             UInt64,
    is_deleted           UInt8
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS gold.customer_ltv_queue (
    op     String,
    before Nullable(String),
    after  Nullable(String)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'dbz-kafka:9092',
         kafka_topic_list = 'gold.customer_ltv',
         kafka_group_name = 'clickhouse-gold-customer_ltv',
         kafka_format = 'JSONEachRow',
         kafka_flush_interval_ms = 1000,
         input_format_json_read_objects_as_strings = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.customer_ltv_mv TO gold.customer_ltv AS
WITH ifNull(after, before) AS r
SELECT
    toInt32(JSONExtractInt(r, 'customer_id'))   AS customer_id,
    JSONExtractString(r, 'email')               AS email,
    JSONExtractString(r, 'country')             AS country,
    JSONExtractUInt(r, 'orders_count')          AS orders_count,
    JSONExtractInt(r, 'lifetime_value_cents')   AS lifetime_value_cents,
    _offset                                     AS _version,
    op = 'd'                                    AS is_deleted
FROM gold.customer_ltv_queue;
