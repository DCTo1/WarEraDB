-- =============================================
--  Performance indexes (adjust as needed)
-- =============================================

-- Unique index on MongoDB _id (must be an index, not a constraint,
-- because TimescaleDB hypertables require constraints to include
-- the partitioning column)
CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_transaction_id ON transactions (transaction_id);

CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_seller_id ON transactions (seller_id);
CREATE INDEX IF NOT EXISTS idx_transactions_buyer_id ON transactions (buyer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_type_id ON transactions (transaction_type_id);
