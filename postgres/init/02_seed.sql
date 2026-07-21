-- Identical seed data to stream-cdc-peerdb's postgres/init/02_seed.sql,
-- deliberately: same source rows means any difference observed downstream
-- comes from the CDC middleware, not from different test data. Volumes are
-- intentionally small (this is a POC, not a load test) but the shape is
-- realistic: multiple categories, a product catalog, customers, multi-item
-- orders, and a payment per order at a plausible order/payment status mix.

INSERT INTO categories (name) VALUES
    ('Electronics'), ('Home & Kitchen'), ('Books'), ('Sportswear'), ('Toys');

INSERT INTO customers (email, first_name, last_name, country) VALUES
    ('amina.kone@example.com',     'Amina',   'Kone',      'SN'),
    ('luca.rossi@example.com',     'Luca',    'Rossi',     'IT'),
    ('mei.chen@example.com',       'Mei',     'Chen',      'CN'),
    ('john.smith@example.com',     'John',    'Smith',     'US'),
    ('fatima.zahra@example.com',   'Fatima',  'Zahra',     'MA'),
    ('erik.johansson@example.com', 'Erik',    'Johansson', 'SE'),
    ('priya.patel@example.com',    'Priya',   'Patel',     'IN'),
    ('carlos.mendez@example.com',  'Carlos',  'Mendez',    'MX'),
    ('yuki.tanaka@example.com',    'Yuki',    'Tanaka',    'JP'),
    ('sophie.dubois@example.com',  'Sophie',  'Dubois',    'FR'),
    ('oliver.brown@example.com',   'Oliver',  'Brown',     'GB'),
    ('ana.silva@example.com',      'Ana',     'Silva',     'BR'),
    ('lars.nielsen@example.com',   'Lars',    'Nielsen',   'DK'),
    ('grace.osei@example.com',     'Grace',   'Osei',      'GH'),
    ('daniel.kim@example.com',     'Daniel',  'Kim',       'KR');

INSERT INTO products (sku, name, category_id, price_cents, description) VALUES
    ('ELEC-001', 'Wireless Noise-Cancelling Headphones', 1, 24999, 'Over-ear Bluetooth headphones with ANC'),
    ('ELEC-002', '65W USB-C Fast Charger',                1,  3499, 'GaN charger, dual port'),
    ('ELEC-003', '4K Action Camera',                      1, 18999, 'Waterproof action camera with stabilization'),
    ('ELEC-004', 'Mechanical Keyboard',                   1,  8999, 'Hot-swappable mechanical keyboard'),
    ('ELEC-005', 'Portable SSD 1TB',                      1, 10999, 'USB-C portable solid state drive'),
    ('HOME-001', 'Stainless Steel French Press',          2,  2999, '1L coffee press'),
    ('HOME-002', 'Non-Stick Frying Pan 28cm',              2,  3299, 'Induction-compatible non-stick pan'),
    ('HOME-003', 'Ceramic Dinnerware Set (16pc)',          2,  6499, 'Service for four'),
    ('HOME-004', 'Robot Vacuum Cleaner',                   2, 22999, 'App-controlled robot vacuum'),
    ('BOOK-001', 'Designing Data-Intensive Applications',  3,  4499, 'Martin Kleppmann'),
    ('BOOK-002', 'The Pragmatic Programmer',               3,  3999, 'Hunt & Thomas'),
    ('BOOK-003', 'Atomic Habits',                          3,  2499, 'James Clear'),
    ('SPRT-001', 'Running Shoes',                          4,  8999, 'Lightweight road running shoes'),
    ('SPRT-002', 'Yoga Mat',                                4,  2299, '6mm non-slip yoga mat'),
    ('SPRT-003', 'Adjustable Dumbbell Set',                 4, 15999, '2x adjustable dumbbells, 5-25kg'),
    ('TOY-001',  'Building Blocks Set (500pc)',             5,  4999, 'Compatible building block set'),
    ('TOY-002',  'Remote Control Car',                      5,  5999, '1:16 scale RC car'),
    ('TOY-003',  'Board Game: Strategy Classic',            5,  3499, '2-4 player strategy board game'),
    ('ELEC-006', 'Smartwatch',                              1, 19999, 'Fitness tracking smartwatch'),
    ('HOME-005', 'LED Desk Lamp',                           2,  2799, 'Dimmable LED desk lamp with USB port');

-- 30 orders spread across customers, each with 1-3 line items, and a
-- matching payment row. Written explicitly (not generate_series) so the
-- data + totals stay internally consistent and easy to reason about when
-- verifying the pipeline later.
INSERT INTO orders (customer_id, status, order_total_cents, created_at) VALUES
    (1,  'delivered', 24999, now() - interval '30 days'),
    (2,  'delivered',  6498, now() - interval '29 days'),
    (3,  'shipped',   18999, now() - interval '25 days'),
    (4,  'delivered', 12498, now() - interval '24 days'),
    (5,  'paid',      22999, now() - interval '20 days'),
    (6,  'delivered',  8999, now() - interval '19 days'),
    (7,  'cancelled',  3499, now() - interval '18 days'),
    (8,  'delivered', 10999, now() - interval '17 days'),
    (9,  'shipped',   28998, now() - interval '15 days'),
    (10, 'delivered',  4499, now() - interval '14 days'),
    (11, 'paid',       8999, now() - interval '13 days'),
    (12, 'delivered', 15999, now() - interval '12 days'),
    (13, 'delivered',  6498, now() - interval '11 days'),
    (14, 'shipped',   19999, now() - interval '10 days'),
    (15, 'paid',       4999, now() - interval '9 days'),
    (1,  'delivered',  5999, now() - interval '8 days'),
    (2,  'pending',    3499, now() - interval '7 days'),
    (3,  'delivered',  2999, now() - interval '6 days'),
    (4,  'paid',       3299, now() - interval '6 days'),
    (5,  'delivered',  6499, now() - interval '5 days'),
    (6,  'shipped',   22999, now() - interval '5 days'),
    (7,  'delivered',  4499, now() - interval '4 days'),
    (8,  'paid',       3999, now() - interval '4 days'),
    (9,  'delivered',  2499, now() - interval '3 days'),
    (10, 'shipped',    8999, now() - interval '3 days'),
    (11, 'pending',    2299, now() - interval '2 days'),
    (12, 'delivered', 15999, now() - interval '2 days'),
    (13, 'paid',       4999, now() - interval '1 days'),
    (14, 'pending',    5999, now() - interval '1 days'),
    (15, 'delivered',  3499, now());

INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents) VALUES
    (1,  1, 1, 24999),
    (2,  6, 1,  2999), (2, 15, 1, 3499),
    (3,  3, 1, 18999),
    (4, 10, 1,  4499), (4, 11, 1, 3999), (4, 14, 1, 4000),
    (5,  9, 1, 22999),
    (6, 13, 1,  8999),
    (7,  6, 1,  3499),
    (8,  5, 1, 10999),
    (9,  1, 1, 24999), (9,  2, 1, 3999),
    (10, 10, 1, 4499),
    (11, 13, 1, 8999),
    (12, 15, 1, 15999),
    (13,  7, 1, 3299), (13, 18, 1, 3199),
    (14, 19, 1, 19999),
    (15, 16, 1, 4999),
    (16, 17, 1, 5999),
    (17,  7, 1, 3499),
    (18,  6, 1, 2999),
    (19,  7, 1, 3299),
    (20,  8, 1, 6499),
    (21,  9, 1, 22999),
    (22, 10, 1, 4499),
    (23, 11, 1, 3999),
    (24, 12, 1, 2499),
    (25, 13, 1, 8999),
    (26, 14, 1, 2299),
    (27, 15, 1, 15999),
    (28, 16, 1, 4999),
    (29, 17, 1, 5999),
    (30,  6, 1, 3499);

INSERT INTO payments (order_id, amount_cents, method, status, processed_at)
SELECT
    o.order_id,
    o.order_total_cents,
    (ARRAY['card','paypal','bank_transfer'])[1 + (o.order_id % 3)],
    CASE o.status
        WHEN 'cancelled' THEN 'refunded'
        WHEN 'pending'   THEN 'pending'
        ELSE 'succeeded'
    END,
    CASE WHEN o.status = 'pending' THEN NULL ELSE o.created_at + interval '5 minutes' END
FROM orders o;
