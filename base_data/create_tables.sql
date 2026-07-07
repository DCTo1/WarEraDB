-- 1. Lookup tables (small, heavily cached)
CREATE TABLE inventory_ids (
    id SERIAL PRIMARY KEY,
    external_id UUID UNIQUE NOT NULL -- MongoDB ObjectID encoded as UUID (12 bytes + 4 zero bytes)
);

CREATE TABLE item_codes (
    id SMALLSERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL      -- 'sniper', 'scraps', 'case1', etc.
);

CREATE TABLE transaction_types (
    id SMALLSERIAL PRIMARY KEY,
    type TEXT UNIQUE NOT NULL      -- 'trading', 'itemMarket', 'wage', etc.
);

-- 1.5. Items table (normalized item data with skills)
CREATE TABLE items (
    id BIGSERIAL PRIMARY KEY,
    item_uuid UUID UNIQUE NOT NULL,           -- MongoDB _id from the item, encoded as UUID
    item_code_id SMALLINT NOT NULL REFERENCES item_codes(id), -- the actual item code
    primary_skill SMALLINT NULL,
    secondary_skill SMALLINT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_acquisition_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_items_code_skills ON items(item_code_id, primary_skill, secondary_skill);
CREATE INDEX IF NOT EXISTS idx_items_uuid ON items(item_uuid);

-- 2. Main hypertable
CREATE TABLE transactions (
    -- id BIGSERIAL was removed — the unique index (transaction_id, created_at) is the effective PK.
    
    -- MongoDB _id (unique identifier from the source API).
    -- Uniqueness enforced via a unique composite INDEX
    -- (transaction_id, created_at) because TimescaleDB requires
    -- unique indexes to include the partitioning column.
    transaction_id UUID NOT NULL,
    
    -- Time columns
    created_at TIMESTAMPTZ NOT NULL, -- when the transaction was recorded
    offer_created_at TIMESTAMPTZ NULL,    -- when the offer was placed (from JSON payload)
    
    -- Normalized FKs (nullable)
    seller_id INT NULL REFERENCES inventory_ids(id),
    buyer_id INT NULL REFERENCES inventory_ids(id),
    secondary_seller_id INT NULL REFERENCES inventory_ids(id), -- when a MU or Country buys/sells, the user that made the action is seller/buyer
    secondary_buyer_id INT NULL REFERENCES inventory_ids(id),  -- but we also get the MU/Country ID
    item_code_id SMALLINT NULL REFERENCES item_codes(id),  -- what was traded / the case / the input material
    -- result_item_code_id was removed in favour of items.item_code_id.
    -- For openCase / craftItem / dismantleItem, use items.item_code_id to get the result code.
    -- result_item_code_id SMALLINT NULL REFERENCES item_codes(id),
    item_id BIGINT NULL REFERENCES items(id),             -- the item instance (if any)
    transaction_type_id SMALLINT NOT NULL REFERENCES transaction_types(id),
    
    -- Other values
    money DOUBLE PRECISION NULL,
    quantity DOUBLE PRECISION NULL,
    -- primary_skill and secondary_skill now live on the items table.
    -- primary_skill SMALLINT NULL,
    -- secondary_skill SMALLINT NULL,
    
    -- extra JSONB NULL  -- removed: was always NULL after migration 02
);

-- 3. Convert to TimescaleDB hypertable (partitions by time)
SELECT create_hypertable(
    'transactions', 
    'created_at', 
    chunk_time_interval => INTERVAL '1 day'  -- adjust based on volume (start with 1 day)
);
