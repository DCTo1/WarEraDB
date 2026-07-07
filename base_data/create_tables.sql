-- 1. Lookup tables (small, heavily cached)

CREATE TABLE transaction_types (
    id   SMALLSERIAL PRIMARY KEY,
    type TEXT UNIQUE NOT NULL   -- 'trading', 'itemMarket', 'wage', ...
);

CREATE TABLE item_codes (
    id   SMALLSERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL   -- 'sniper', 'scraps', 'case1', ...
);

CREATE TABLE inventory_ids (
    id          SERIAL PRIMARY KEY,
    external_id UUID UNIQUE NOT NULL  -- MongoDB ObjectID encoded as UUID (12 bytes + 4 zero bytes)
);


-- 2. Items table (normalized item instances with skills)
--
-- Columns grouped by alignment: 8B → 4B → 2B

CREATE TABLE items (
    id                  BIGSERIAL PRIMARY KEY,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_acquisition_at TIMESTAMPTZ NULL,
    item_uuid           UUID UNIQUE NOT NULL,              -- MongoDB _id from the item object
    item_code_id        SMALLINT NOT NULL REFERENCES item_codes(id),  -- the actual item's code
    primary_skill       SMALLINT NULL,     -- attack / armor / dodge / criticalDamages
    secondary_skill     SMALLINT NULL      -- criticalChance (NULL for non-weapons)
);

CREATE INDEX IF NOT EXISTS idx_items_code_skills ON items(item_code_id, primary_skill, secondary_skill);
CREATE INDEX IF NOT EXISTS idx_items_uuid ON items(item_uuid);


-- 3. Main hypertable
--
-- Columns grouped by alignment: 8B → 4B → 2B
-- created_at is NOT first in the column list (TimescaleDB does not require it
-- to be first), but it IS the partitioning dimension.
-- The unique index on (transaction_id, created_at) is the effective primary key.

CREATE TABLE transactions (
    -- 8-byte aligned
    created_at          TIMESTAMPTZ NOT NULL,     -- partition column
    offer_created_at    TIMESTAMPTZ NULL,
    money               DOUBLE PRECISION NULL,
    quantity            DOUBLE PRECISION NULL,
    item_id             BIGINT NULL REFERENCES items(id),

    -- 4-byte aligned
    transaction_id      UUID NOT NULL,            -- MongoDB _id encoded as UUID
    seller_id           INT NULL REFERENCES inventory_ids(id),
    buyer_id            INT NULL REFERENCES inventory_ids(id),
    secondary_seller_id INT NULL REFERENCES inventory_ids(id),  -- MU/Country when a user acts for them
    secondary_buyer_id  INT NULL REFERENCES inventory_ids(id),

    -- 2-byte aligned
    item_code_id        SMALLINT NULL REFERENCES item_codes(id),  -- what was traded / the case / the input
    transaction_type_id SMALLINT NOT NULL REFERENCES transaction_types(id)
);


-- 4. Convert to TimescaleDB hypertable (partitions by time)

SELECT create_hypertable(
    'transactions',
    'created_at',
    chunk_time_interval => INTERVAL '1 day'
);
