-- =============================================================================
--  Indexes for the transactions hypertable
--
--  During the initial 70M-row bulk load, only the UNIQUE index is needed
--  (it supports ON CONFLICT in insert_transaction()).  All other indexes
--  are created AFTER the bulk load is complete to avoid index-maintenance
--  overhead on every INSERT.
--
--  Run this file after the bulk load finishes, or run individual CREATE
--  INDEX commands as needed once query patterns are known.
-- =============================================================================

-- Unique index — REQUIRED during bulk insert for ON CONFLICT.
-- TimescaleDB requires the partitioning column in unique indexes.
CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_transaction_id
    ON transactions (transaction_id, created_at);

-- Post-load indexes (create after bulk insert is done):
--   CREATE INDEX CONCURRENTLY idx_transactions_created_at
--       ON transactions (created_at DESC);
--   CREATE INDEX CONCURRENTLY idx_transactions_seller_id
--       ON transactions (seller_id);
--   CREATE INDEX CONCURRENTLY idx_transactions_buyer_id
--       ON transactions (buyer_id);
--   CREATE INDEX CONCURRENTLY idx_transactions_type_id
--       ON transactions (transaction_type_id);
