-- Hand-written destination DDL -- the single clearest concrete contrast
-- with PeerDB, which auto-generates this from the source table on
-- `CREATE MIRROR`. Here, the ClickHouse Kafka Connect sink connector does
-- NOT create tables -- these must exist before the connector starts. With
-- `auto.evolve=true` (connectors/ch-sink-connector.json) it does add
-- columns that appear on the source later, but only adds: a renamed or
-- dropped source column is still the operator's job (see
-- docs/architecture.md's schema-evolution findings).
--
-- Column shape: verified against the connector's own source
-- (DebeziumRecordConvertor.java) rather than assumed. With
-- `debeziumCDCEnabled=true` (connectors/ch-sink-connector.json), the sink
-- consumes Debezium's raw envelope (op/before/after/source) directly --
-- no flattening SMT needed -- and injects two columns per row:
--   _version    UInt64 -- the PostgreSQL WAL LSN the change committed at
--                          (source.lsn), used as ReplacingMergeTree's
--                          version column. This is a genuine improvement
--                          over a millisecond timestamp: LSN is strictly
--                          monotonic per source, so two updates to the
--                          same row can never tie the way two updates in
--                          the same millisecond could. Plays the same role
--                          as PeerDB's `_peerdb_version`.
--   is_deleted  UInt8  -- 0 for insert/update/snapshot rows, 1 for
--                          deletes. Plays the same role as PeerDB's
--                          `_peerdb_is_deleted` -- same query gotcha
--                          applies: every read needs
--                          `FINAL WHERE is_deleted = 0`.

CREATE TABLE IF NOT EXISTS debezium_cdc.categories (
    category_id  Int32,
    name         String,
    created_at   DateTime64(6),
    _version     UInt64,
    is_deleted   UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS debezium_cdc.customers (
    customer_id  Int32,
    email        String,
    first_name   String,
    last_name    String,
    country      String,
    created_at   DateTime64(6),
    updated_at   DateTime64(6),
    _version     UInt64,
    is_deleted   UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS debezium_cdc.products (
    product_id   Int32,
    sku          String,
    name         String,
    category_id  Int32,
    price_cents  Int32,
    description  Nullable(String),
    created_at   DateTime64(6),
    updated_at   DateTime64(6),
    _version     UInt64,
    is_deleted   UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS debezium_cdc.orders (
    order_id           Int32,
    customer_id        Int32,
    status             LowCardinality(String),
    order_total_cents  Int32,
    created_at         DateTime64(6),
    updated_at         DateTime64(6),
    _version           UInt64,
    is_deleted         UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS debezium_cdc.order_items (
    order_item_id     Int32,
    order_id          Int32,
    product_id        Int32,
    quantity           Int32,
    unit_price_cents  Int32,
    created_at        DateTime64(6),
    _version          UInt64,
    is_deleted        UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS debezium_cdc.payments (
    payment_id    Int32,
    order_id      Int32,
    amount_cents  Int32,
    method        LowCardinality(String),
    status        LowCardinality(String),
    processed_at  Nullable(DateTime64(6)),
    _version      UInt64,
    is_deleted    UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
ORDER BY payment_id;
