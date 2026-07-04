-- Initialisation script — runs once on first container start.
-- Creates the lakehouse schema, Debezium CDC objects, and sample data.

-- ── Tables ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    name        VARCHAR(255)  NOT NULL,
    email       VARCHAR(255)  NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id    BIGSERIAL PRIMARY KEY,
    customer_id BIGINT        NOT NULL REFERENCES public.customers(customer_id),
    status      VARCHAR(50)   NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(12,2) NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.order_items (
    item_id     BIGSERIAL PRIMARY KEY,
    order_id    BIGINT        NOT NULL REFERENCES public.orders(order_id),
    product_id  BIGINT        NOT NULL,
    quantity    INT           NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ── Debezium CDC objects ───────────────────────────────────────────────────────

-- Heartbeat table prevents WAL accumulation on idle monitored tables.
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
    id     INT         PRIMARY KEY DEFAULT 1,
    ts_ms  TIMESTAMPTZ NOT NULL    DEFAULT NOW()
);
INSERT INTO public.debezium_heartbeat VALUES (1) ON CONFLICT DO NOTHING;

-- Replication role — password is set via DEBEZIUM_PASSWORD env var in init.sh.
-- Do NOT hardcode credentials here.
CREATE ROLE debezium WITH LOGIN REPLICATION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT UPDATE ON public.debezium_heartbeat TO debezium;

-- Publication covers exactly the tables Debezium is configured to capture.
CREATE PUBLICATION dbz_publication
    FOR TABLE public.orders, public.customers, public.order_items;

-- ── Sample data (optional, for smoke-testing the pipeline) ────────────────────

INSERT INTO public.customers (name, email) VALUES
    ('Alice Martin',  'alice@example.com'),
    ('Bob Dupont',    'bob@example.com');

INSERT INTO public.orders (customer_id, status, total_amount) VALUES
    (1, 'completed', 129.99),
    (2, 'pending',   49.50);

INSERT INTO public.order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 101, 2, 49.99),
    (1, 202, 1, 30.01),
    (2, 303, 1, 49.50);
