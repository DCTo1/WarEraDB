-- =============================================
--  Performance indexes (adjust as needed)
-- =============================================

-- Unique index on transaction UUID (converted from MongoDB _id hex).
-- TimescaleDB requires the partitioning column to be included, so we
-- create a composite unique index on (transaction_id, created_at).
CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_transaction_id ON transactions (transaction_id, created_at);

CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_seller_id ON transactions (seller_id);
CREATE INDEX IF NOT EXISTS idx_transactions_buyer_id ON transactions (buyer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_type_id ON transactions (transaction_type_id);
