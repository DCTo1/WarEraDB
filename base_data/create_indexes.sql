-- =============================================================================
-- WarEraDB optional query indexes
--
-- NOTHING here is required: the unique indexes needed for the upsert
-- ON CONFLICT targets live in create_tables.sql, and every query works
-- without these (they only speed up specific lookups). Uncomment the ones
-- you actually use — e.g. skip idx_battles_war if you never filter by war.
--
-- Each index costs write amplification on INSERT/UPDATE, so keep the set
-- as small as your queries need.
-- =============================================================================

-- Items
-- Covering index for skill/code queries (which items of a code exist, and
-- with which skills).
-- CREATE INDEX IF NOT EXISTS idx_items_code_skills
--     ON items(item_code_id, primary_skill, secondary_skill);

-- =============================================================================
--  Battles
--
--  (the battles/rounds UNIQUE + PK constraints are inline in create_tables.sql)
-- =============================================================================

-- Battle list pages ordered newest-first (db_web battles tab)
-- CREATE INDEX IF NOT EXISTS idx_battles_created_at ON battles (created_at DESC);

-- Filters on finished/active battles (ended_at IS NULL)
-- CREATE INDEX IF NOT EXISTS idx_battles_ended_at ON battles (ended_at);

-- Country filter (attacker/defender) — db_web country filter, battle views
-- CREATE INDEX IF NOT EXISTS idx_battles_country
--     ON battles (attacker_country_id, defender_country_id);

-- Battle type filter (war / resistance / tournament / revolution)
-- CREATE INDEX IF NOT EXISTS idx_battles_type ON battles (type_id);

-- Wars history / per-war breakdowns
-- CREATE INDEX IF NOT EXISTS idx_battles_war ON battles (war_id);

-- Tournament stage pages
-- CREATE INDEX IF NOT EXISTS idx_battles_tournament ON battles (tournament_id);

-- Rounds of a battle (round_details / per-battle round pages)
-- CREATE INDEX IF NOT EXISTS idx_rounds_battle ON rounds (battle_id);

-- Newest rounds first
-- CREATE INDEX IF NOT EXISTS idx_rounds_created ON rounds (created_at DESC);

-- Winner lookups (which battles a country won)
-- CREATE INDEX IF NOT EXISTS idx_rounds_winner ON rounds (won_by_country_id);

-- Bounty effective-at lookups
-- CREATE INDEX IF NOT EXISTS idx_battle_bounties_effective
--     ON battle_bounties (bounty_effective_at);

-- =============================================================================
--  Ranking entries
--
--  The unique keys (created_at, battle_id, ...) are the ON CONFLICT targets
--  and live in create_tables.sql. On compressed chunks indexes are dropped,
--  so these only exist on uncompressed (recent) chunks; queries over old
--  data rely on the compress_orderby (battle_id, created_at) segment
--  skipping instead.
-- =============================================================================

-- Per-entity history (db_web /user pages: an entity's top battles). Useful
-- only while data is recent — compressed chunks ignore it.
-- CREATE INDEX IF NOT EXISTS idx_battle_ranking_entity
--     ON battle_ranking_entries (entity_id, side, entity_type);

-- Per-battle lookups for the ranking pipeline's cleanup joins
-- (single-battle DELETEs). The hypertable unique key starts with
-- created_at, so battle_id is not covered by it.
-- CREATE INDEX IF NOT EXISTS idx_battle_ranking_battle
--     ON battle_ranking_entries (battle_id);
-- CREATE INDEX IF NOT EXISTS idx_round_ranking_battle
--     ON round_ranking_entries (battle_id);

-- =============================================================================
--  Endpoint usage
--
--  endpoints_used is append-only, but rollup_endpoint_usage.py (throttled to
--  ~once/day) folds rows older than a few days into endpoint_usage_daily /
--  endpoint_usage_daily_totals and deletes them, so the raw table stays
--  small and recent-only instead of growing unbounded (measured 2026-08-13,
--  pre-rollup: 3.5 M rows / 271 MB after 9 days). Because the remaining raw
--  rows are always recent, a BRIN index on date_used is now worth it: it's
--  tiny, near-free to maintain given date_used is perfectly correlated with
--  physical insert order, and lets both the /stats "last 24h" query and the
--  rollup function's own `WHERE date_used < cutoff` scan/delete prune chunks
--  instead of seq-scanning. The per-endpoint / per-day btree indexes below
--  stay commented out — the raw table's queries all filter by date_used
--  first (or are the whole-table rollup GROUP BY, which reads every
--  surviving row no matter what), so a second index would only tax inserts.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_endpoints_used_date_brin
    ON endpoints_used USING BRIN (date_used);

-- Per-endpoint history (counts, last used) — covers the stats page's main
-- GROUP BY when the log is large.
-- CREATE INDEX IF NOT EXISTS idx_endpoints_used_endpoint
--     ON endpoints_used (endpoint_id, date_used);

-- Per-day trends (calls per day / per hour)
-- CREATE INDEX IF NOT EXISTS idx_endpoints_used_date
--     ON endpoints_used (date_used);

-- =============================================================================
--  Users
--
--  The user-lite picker queries users by activity window: last_active_at >
--  now - 4 days, ordered by lite_checked_at age + wealth. A 101K-row seq
--  scan + sort is a few ms, so nothing here is needed yet.
-- =============================================================================

-- Active-pool picker (users active within 4 days)
-- CREATE INDEX IF NOT EXISTS idx_users_last_active
--     ON users (last_active_at);

-- =============================================================================
--  Transactions
--
--  ENABLED BY DEFAULT (migration_20, 2026-08-08) — the viewer's /transactions
--  and /user pages depend on them. They live ONLY on the uncompressed day-
--  chunks: TimescaleDB drops per-chunk indexes when a chunk compresses, so
--  compressed chunks serve the type filter via the segmentby and the sort via
--  the compress_orderby, while the 1-72 h window scans hit the ~7 recent
--  uncompressed chunks (append-mostly inserts — low write amplification).
--  Rationale: migration_19 dropped these as redundant based on a 2M-row-TOTAL
--  benchmark where a 24 h window scanned only ~22K rows; the window alone now
--  holds ~2M rows (~700K/day) and the unindexed scan + top-N sort measured
--  53 ms (browse) / 81 ms (histogram) on 2026-08-08 — 0.05 ms / 46 ms with
--  them. The unique (transaction_id, created_at) upsert index lives in
--  create_tables.sql.
-- =============================================================================

-- Default browse: ORDER BY created_at DESC LIMIT 100 over the window
CREATE INDEX IF NOT EXISTS idx_transactions_created_at
    ON transactions (created_at DESC);

-- Type filter + the per-type histogram (window-scan group by)
CREATE INDEX IF NOT EXISTS idx_transactions_type
    ON transactions (transaction_type_id, created_at DESC);

-- User filter (seller/buyer) on /transactions + the /user page's recent box
CREATE INDEX IF NOT EXISTS idx_transactions_seller
    ON transactions (seller_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_buyer
    ON transactions (buyer_id, created_at DESC);
-- =============================================================================

