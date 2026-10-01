-- Gold layer: three continuously-maintained aggregates computed by Flink
-- from Debezium's change topics, written back to Kafka as changelog
-- topics (gold.*) that ClickHouse ingests (clickhouse/gold.sql).
--
-- Why Flink and not a ClickHouse materialized view: a ClickHouse MV only
-- ever sees newly-inserted rows, so an UPDATE (order pending -> cancelled)
-- would be counted again instead of replacing the old value, a DELETE
-- would never be subtracted, and a join would not react to a change on
-- its right-hand side. Flink reads `debezium-json` as a changelog: every
-- update arrives as a retraction of the old row plus the new row, and
-- every aggregate and join below retracts/re-emits its result to match.
--
-- That retraction needs the full old row, which is why every source table
-- is `REPLICA IDENTITY FULL` (postgres/init/01_schema.sql). With
-- Postgres's default identity Debezium's `before` is null on UPDATE, and
-- Flink's debezium-json format refuses to process it.
--
-- Submitted by scripts/deploy_gold.sh as one job (one STATEMENT SET), so
-- the three queries share a single read of each source topic.

SET 'pipeline.name' = 'ecommerce-gold';

-- ── sources: Debezium change topics ──────────────────────────────────────────
-- Timestamps stay STRING: Debezium encodes TIMESTAMPTZ as an ISO-8601 UTC
-- string ("2026-09-02T10:11:12.123456Z"), so the first 10 characters are
-- already the UTC date. No PRIMARY KEY clauses: the kafka connector
-- rejects them for debezium-json, since it can't guarantee uniqueness.

CREATE TABLE orders (
    order_id          INT,
    customer_id       INT,
    status            STRING,
    order_total_cents INT,
    created_at        STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecommerce.public.orders',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'properties.group.id' = 'flink-ecommerce-gold',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    -- The source connector uses JsonConverter with schemas.enable=true,
    -- so each message is {"schema": ..., "payload": <envelope>}.
    'debezium-json.schema-include' = 'true'
);

CREATE TABLE order_items (
    order_item_id    INT,
    order_id         INT,
    product_id       INT,
    quantity         INT,
    unit_price_cents INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecommerce.public.order_items',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'properties.group.id' = 'flink-ecommerce-gold',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true'
);

CREATE TABLE products (
    product_id  INT,
    category_id INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecommerce.public.products',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'properties.group.id' = 'flink-ecommerce-gold',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true'
);

CREATE TABLE categories (
    category_id INT,
    name        STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecommerce.public.categories',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'properties.group.id' = 'flink-ecommerce-gold',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true'
);

CREATE TABLE customers (
    customer_id INT,
    email       STRING,
    country     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecommerce.public.customers',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'properties.group.id' = 'flink-ecommerce-gold',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true'
);

-- ── sinks: gold changelog topics ─────────────────────────────────────────────
-- Written as debezium-json too: an insert is {"op":"c","after":{...}}, a
-- retraction is {"op":"d","before":{...}}. Unlike upsert-kafka, this never
-- produces null-value tombstones, which neither the ClickHouse Kafka
-- engine nor the ClickHouse Kafka Connect sink can turn into a delete.
-- 'key.fields' partitions by the gold row's key, so every change to one
-- gold row lands in one partition, in order -- ClickHouse relies on that
-- ordering (it uses the Kafka offset as the row version).

CREATE TABLE gold_daily_revenue (
    order_date    DATE,
    orders_count  BIGINT,
    revenue_cents BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'gold.daily_revenue',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'key.format' = 'json',
    'key.fields' = 'order_date',
    'value.format' = 'debezium-json'
);

CREATE TABLE gold_category_revenue (
    category_id   INT,
    category_name STRING,
    units_sold    BIGINT,
    revenue_cents BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'gold.category_revenue',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'key.format' = 'json',
    'key.fields' = 'category_id',
    'value.format' = 'debezium-json'
);

CREATE TABLE gold_customer_ltv (
    customer_id          INT,
    email                STRING,
    country              STRING,
    orders_count         BIGINT,
    lifetime_value_cents BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'gold.customer_ltv',
    'properties.bootstrap.servers' = 'dbz-kafka:9092',
    'key.format' = 'json',
    'key.fields' = 'customer_id',
    'value.format' = 'debezium-json'
);

-- ── the gold queries ─────────────────────────────────────────────────────────
-- Each query aggregates on the gold row's own key first and joins
-- dimension attributes (category name, customer email/country) on
-- afterwards. Grouping by a mutable attribute instead would turn a
-- rename into "delete row under old key, insert under new key", two
-- changes that could reach the sink out of order once parallelism > 1.
--
-- scripts/verify_cdc.sh recomputes each of these directly in Postgres
-- and requires ClickHouse's gold tables to match exactly.

EXECUTE STATEMENT SET
BEGIN

-- Revenue per order day (UTC), cancelled orders excluded. Cancelling an
-- order retracts it from its day.
INSERT INTO gold_daily_revenue
SELECT
    CAST(SUBSTRING(created_at FROM 1 FOR 10) AS DATE) AS order_date,
    COUNT(*)                                         AS orders_count,
    SUM(CAST(order_total_cents AS BIGINT))           AS revenue_cents
FROM orders
WHERE status <> 'cancelled'
GROUP BY CAST(SUBSTRING(created_at FROM 1 FOR 10) AS DATE);

-- Units and revenue per category from line items: a three-way join
-- (items -> orders for status, items -> products for category), then the
-- category name joined on. Reacts to item deletes, order cancellations,
-- products moving category, and category renames.
INSERT INTO gold_category_revenue
SELECT
    c.category_id,
    c.name AS category_name,
    s.units_sold,
    s.revenue_cents
FROM (
    SELECT
        p.category_id,
        SUM(CAST(oi.quantity AS BIGINT))                      AS units_sold,
        SUM(CAST(oi.quantity AS BIGINT) * oi.unit_price_cents) AS revenue_cents
    FROM order_items AS oi
    JOIN orders   AS o ON o.order_id = oi.order_id
    JOIN products AS p ON p.product_id = oi.product_id
    WHERE o.status <> 'cancelled'
    GROUP BY p.category_id
) AS s
JOIN categories AS c ON c.category_id = s.category_id;

-- One row per customer, including customers with no orders yet (zeros).
-- A change to the customer's country updates their row in place.
INSERT INTO gold_customer_ltv
SELECT
    cu.customer_id,
    cu.email,
    cu.country,
    COALESCE(a.orders_count, 0)         AS orders_count,
    COALESCE(a.lifetime_value_cents, 0) AS lifetime_value_cents
FROM customers AS cu
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*)                               AS orders_count,
        SUM(CAST(order_total_cents AS BIGINT)) AS lifetime_value_cents
    FROM orders
    WHERE status <> 'cancelled'
    GROUP BY customer_id
) AS a ON a.customer_id = cu.customer_id;

END;
