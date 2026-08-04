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
--  endpoints_used is append-only; the stats page aggregates it. Indexes only
--  matter once it holds a lot of rows.
-- =============================================================================

-- Per-endpoint history (counts, last used) — covers the stats page's main
-- GROUP BY when the log is large.
-- CREATE INDEX IF NOT EXISTS idx_endpoints_used_endpoint
--     ON endpoints_used (endpoint_id, date_used);

-- Per-day trends (calls per day / per hour)
-- CREATE INDEX IF NOT EXISTS idx_endpoints_used_date
--     ON endpoints_used (date_used);
