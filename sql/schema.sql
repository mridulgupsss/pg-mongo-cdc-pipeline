-- Source schema: e-commerce orders (parent) + order_items (child, FK to orders).
-- Both tables have >=6 mixed-type columns with realistic names, as required by 3.1.

CREATE TYPE order_status AS ENUM ('pending', 'paid', 'shipped', 'delivered', 'cancelled');

CREATE TABLE IF NOT EXISTS orders (
    order_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_email  TEXT          NOT NULL,
    total_amount    NUMERIC(12,2) NOT NULL DEFAULT 0,
    status          order_status  NOT NULL DEFAULT 'pending',
    is_paid         BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id      BIGINT        NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_name  TEXT          NOT NULL,
    quantity      INTEGER       NOT NULL DEFAULT 1,
    unit_price    NUMERIC(10,2) NOT NULL DEFAULT 0,
    is_gift       BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);

-- REPLICA IDENTITY FULL: makes UPDATE/DELETE WAL records carry the full OLD row,
-- which we need for byte-for-byte field parity (T5) and reliable delete keys.
-- Trade-off: larger WAL volume under load (documented in DESIGN tuning table).
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- NOTE: we use the wal2json output plugin, so table filtering is done at slot
-- start time via the `add-tables` option (see pipeline/reader.py), not via a
-- PostgreSQL publication (which only applies to the built-in pgoutput plugin).
