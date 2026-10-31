-- 001_init_schema.sql
-- Automated PostgreSQL Schema Initialization & Catalog Seed

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Status Enums
DO $$ BEGIN
    CREATE TYPE order_status AS ENUM ('RESERVED', 'CONFIRMED', 'CANCELLED', 'FAILED');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE transaction_type AS ENUM ('ALLOCATION', 'RESERVATION', 'RELEASE', 'COMPENSATION');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- 2. Items Table
CREATE TABLE IF NOT EXISTS items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku VARCHAR(64) UNIQUE NOT NULL,
    title VARCHAR(255) NOT NULL,
    category VARCHAR(64) NOT NULL,
    total_stock INT NOT NULL CHECK (total_stock >= 0),
    reserved_stock INT NOT NULL DEFAULT 0 CHECK (reserved_stock <= total_stock),
    price_cents INT NOT NULL CHECK (price_cents >= 0),
    sale_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sale_end TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '2 hours',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Orders Table
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_number VARCHAR(64) UNIQUE NOT NULL,
    user_id UUID NOT NULL,
    item_id UUID NOT NULL REFERENCES items(id) ON DELETE RESTRICT,
    status order_status NOT NULL DEFAULT 'RESERVED',
    idempotency_key VARCHAR(128) UNIQUE,
    amount_cents INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Business Rule: Enforce 1 purchase per user per flash-sale item
    CONSTRAINT uq_user_item UNIQUE (user_id, item_id)
);

-- 4. Inventory Audit Log Table (Immutable Ledger)
CREATE TABLE IF NOT EXISTS inventory_audit_log (
    id BIGSERIAL PRIMARY KEY,
    item_id UUID NOT NULL REFERENCES items(id),
    order_id UUID REFERENCES orders(id),
    delta INT NOT NULL,
    type transaction_type NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Indexes
CREATE INDEX IF NOT EXISTS idx_items_sku ON items (sku);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_item_id ON inventory_audit_log (item_id, created_at DESC);

-- 6. Initial Seed Catalog (Agoda, Shopee, Careem & Benchmark)
INSERT INTO items (id, sku, title, category, total_stock, reserved_stock, price_cents)
VALUES 
    (
        'a0000000-0000-0000-0000-000000000001',
        'AGODA-MALDIVES-VILLA-DROP',
        'Agoda Exclusive: Overwater Villa Maldives (3 Nights) — 90% Flash Drop',
        'TRAVEL_HOTEL',
        10,
        0,
        14900
    ),
    (
        'b0000000-0000-0000-0000-000000000002',
        'SHOPEE-1111-IPHONE16-MAX',
        'Shopee 11.11 Mega Drop: iPhone 16 Pro Max 256GB Desert Titanium',
        'ELECTRONICS',
        50,
        0,
        89900
    ),
    (
        'c0000000-0000-0000-0000-000000000003',
        'CAREEM-DUBAI-SUPERCAR-PASS',
        'Careem Dubai SuperCar Chauffeur VIP Pass (McLaren / Ferrari Transfer)',
        'MOBILITY_SERVICES',
        25,
        0,
        4900
    ),
    (
        'd0000000-0000-0000-0000-000000000004',
        'CONCURRENCY-STRESS-BENCHMARK-ITEM',
        'High-Concurrency Flash-Sale Benchmark Item (50 Units)',
        'BENCHMARK',
        50,
        0,
        1000
    )
ON CONFLICT (sku) DO NOTHING;

