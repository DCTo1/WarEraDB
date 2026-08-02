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

-- =============================================================================
--  Battle indexes (Phase 1 of the battle expansion)
--
--  UNIQUE indexes are declared inline in create_tables.sql (required for the
--  ON CONFLICT in insert_battle()/insert_round()). The indexes below are for
--  querying and should be created AFTER the bulk load from
--  extra/battles_cache/ finishes.
-- =============================================================================

--   CREATE INDEX CONCURRENTLY idx_battles_created_at ON battles (created_at DESC);
--   CREATE INDEX CONCURRENTLY idx_battles_ended_at   ON battles (ended_at);
--   CREATE INDEX CONCURRENTLY idx_battles_country    ON battles (attacker_country_id, defender_country_id);
--   CREATE INDEX CONCURRENTLY idx_battles_type       ON battles (type_id);
--   CREATE INDEX CONCURRENTLY idx_battles_war        ON battles (war_id);
--   CREATE INDEX CONCURRENTLY idx_battles_tournament ON battles (tournament_id);

--   CREATE INDEX CONCURRENTLY idx_rounds_battle   ON rounds (battle_id);
--   CREATE INDEX CONCURRENTLY idx_rounds_created  ON rounds (created_at DESC);
--   CREATE INDEX CONCURRENTLY idx_rounds_winner   ON rounds (won_by_country_id);

--   CREATE INDEX CONCURRENTLY idx_battle_bounties_effective ON battle_bounties (bounty_effective_at);
