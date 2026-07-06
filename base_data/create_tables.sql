-- 1. Lookup tables (small, heavily cached)
CREATE TABLE inventory_ids (
    id SERIAL PRIMARY KEY,
    external_id TEXT UNIQUE NOT NULL -- MongoDB UUIDs (e.g., '681cf480...')
);

CREATE TABLE item_codes (
    id SMALLSERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL      -- 'sniper', 'scraps', 'case1', etc.
);

CREATE TABLE transaction_types (
    id SMALLSERIAL PRIMARY KEY,
    type TEXT UNIQUE NOT NULL      -- 'trading', 'itemMarket', 'wage', etc.
);

-- 2. Main hypertable
CREATE TABLE transactions (
    id BIGSERIAL,                  -- auto-incrementing PK
    
    -- MongoDB _id (unique identifier from the source API).
    -- Uniqueness enforced via a unique composite INDEX
    -- (transaction_id, created_at) because TimescaleDB requires
    -- unique indexes to include the partitioning column.
    transaction_id TEXT NOT NULL,
    
    -- Time columns
    created_at TIMESTAMPTZ NOT NULL, -- when the transaction was recorded
    offer_created_at TIMESTAMPTZ NULL,    -- when the offer was placed (from JSON payload)
    
    -- Normalized FKs (nullable)
    seller_id INT NULL REFERENCES inventory_ids(id),
    buyer_id INT NULL REFERENCES inventory_ids(id),
    secondary_seller_id INT NULL REFERENCES inventory_ids(id), -- when a MU or Country buys/sells, the user that made the action is seller/buyer
    secondary_buyer_id INT NULL REFERENCES inventory_ids(id),  -- but we also get the MU/Country ID
    item_code_id SMALLINT NULL REFERENCES item_codes(id),
    result_item_code_id SMALLINT NULL REFERENCES item_codes(id), -- the actual item that was produced (openCase, craftItem, dismantleItem)
    transaction_type_id SMALLINT NOT NULL REFERENCES transaction_types(id),
    
    -- Other values
    money DOUBLE PRECISION NULL,
    quantity DOUBLE PRECISION NULL,
    primary_skill SMALLINT NULL,
    secondary_skill SMALLINT NULL,
    
    -- The dynamic catch-all (stores everything else, is NULL if there is nothing to store)
    extra JSONB NULL
);

-- 3. Convert to TimescaleDB hypertable (partitions by time)
SELECT create_hypertable(
    'transactions', 
    'created_at', 
    chunk_time_interval => INTERVAL '1 day'  -- adjust based on volume (start with 1 day)
);
