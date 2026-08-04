-- =============================================================================
-- WarEraDB schema — clean-start DDL
--
-- 1. Lookup tables
-- 2. Items (normalized instances with skills)
-- 3. Transactions hypertable (1-day chunks)
-- 4. Battle tables: battle_types, battles, rounds, battle_bounties
-- 5. Countries (current-state detail keyed on inventory_ids)
-- 6. Battle/round ranking hypertables (7-day chunks, compressed)
-- 7. Endpoint usage tracking
-- 8. Users
--
-- Only the indexes REQUIRED at insert time live here (unique ON CONFLICT
-- targets, plus the ranking hypertable unique keys). Optional query indexes
-- are commented out in create_indexes.sql — enable what you need. Apply the
-- files in README order:
--   create_tables → functions → item_codes → create_indexes → create_views
-- =============================================================================


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


-- 2. Items (normalized instances with skills)
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


-- 3. Transactions hypertable
--
-- Columns grouped by alignment: 8B → 4B → 2B.
-- created_at is the partitioning dimension (TimescaleDB does not require it
-- to be first). The unique index on (transaction_id, created_at) is the
-- effective primary key — it is also the ON CONFLICT target of
-- insert_transaction(), so it MUST exist before any insert.

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

SELECT create_hypertable(
    'transactions',
    'created_at',
    chunk_time_interval => INTERVAL '1 day'
);

-- TimescaleDB auto-creates an index on the partition column; it is redundant
-- here (created_at lookups go through the unique index below).
DROP INDEX IF EXISTS transactions_created_at_idx;

CREATE UNIQUE INDEX idx_transactions_transaction_id
    ON transactions (transaction_id, created_at);


-- =============================================================================
-- 4. Battle tables
--
-- Design notes:
--   - no is_active column: ended_at IS NULL == active
--   - derived fields (wonBy, roundsToWin) are not stored — compute at query time
--   - isResistance / isTournament covered by battle_types
--   - money/bounty fields live in battle_bounties (per side)
--   - tournament battles/rounds have tournamentTeam instead of country
--   - rounds are stored per rounds[] id, not per roundsHistory entry
-- Columns grouped by alignment: 8B → 4B → 2B.
-- =============================================================================

CREATE TABLE battle_types (
    id   SMALLSERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL   -- 'war', 'resistance', 'tournament', 'revolution'
);

INSERT INTO battle_types (code) VALUES
    ('war'), ('resistance'), ('tournament'), ('revolution')
ON CONFLICT (code) DO NOTHING;

CREATE TABLE battles (
    -- 8-byte aligned
    created_at              TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    ended_at                TIMESTAMPTZ NULL,   -- NULL == battle active (no is_active column)
    attacker_damages        DOUBLE PRECISION NOT NULL,
    defender_damages        DOUBLE PRECISION NOT NULL,

    -- 4-byte aligned
    battle_id                UUID NOT NULL UNIQUE,  -- MongoDB _id (API key, kept alongside the int PK)
    war_id                   UUID NULL,             -- NULL on tournament/revolution battles
    tournament_id            UUID NULL,             -- 'tournament' key = tournament id, NOT a bool
    attacker_country_id      INT NULL REFERENCES inventory_ids(id),  -- NULL on tournament battles; countries trade, so they live in inventory_ids
    attacker_region          UUID NULL,             -- raw ObjectID UUID: regions never trade → no inventory_ids row (no *_id suffix = not a FK)
    attacker_tournament_team UUID NULL,             -- raw ObjectID UUID: temporary tournament teams never trade
    attacker_hit_count       INT NOT NULL,
    defender_country_id      INT NULL REFERENCES inventory_ids(id),  -- NULL on tournament battles
    defender_region          UUID NULL,
    defender_tournament_team UUID NULL,
    defender_hit_count       INT NOT NULL,
    revolution_party_id      INT NULL REFERENCES inventory_ids(id),

    -- 2-byte aligned
    type_id                  SMALLINT NOT NULL REFERENCES battle_types(id),
    tournament_round_number  SMALLINT NULL,   -- tournament stage (0-10), not a round number
    attacker_won_rounds_count SMALLINT NOT NULL,
    defender_won_rounds_count SMALLINT NOT NULL,
    revolution_processed     BOOLEAN NULL,
    is_system_resistance     BOOLEAN NULL,
    is_big_battle            BOOLEAN NULL,

    -- surrogate int PK; referenced by ranking entries / rounds
    id                       SERIAL PRIMARY KEY
);

CREATE TABLE rounds (
    -- 8-byte aligned
    created_at               TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL,
    ended_at                 TIMESTAMPTZ NULL,   -- NULL == round active (no is_active column)
    about_to_end_notified_at TIMESTAMPTZ NULL,
    attacker_damages         DOUBLE PRECISION NOT NULL,
    attacker_points          DOUBLE PRECISION NOT NULL,
    defender_damages         DOUBLE PRECISION NOT NULL,
    defender_points          DOUBLE PRECISION NOT NULL,

    -- 4-byte aligned
    round_id                 UUID NOT NULL UNIQUE,   -- MongoDB _id
    battle_id                UUID NOT NULL REFERENCES battles(battle_id),
    -- round wonBy = side string 'attacker'/'defender' since 2026-01 (country
    -- id before 2025-12). Winner is a country for war/resistance, a team for
    -- tournament rounds — exactly one of the two columns below is set.
    won_by_country_id        INT NULL REFERENCES inventory_ids(id),
    won_by_tournament_team   UUID NULL,              -- raw ObjectID UUID (teams never trade)
    attacker_country_id      INT NULL REFERENCES inventory_ids(id),  -- NULL on tournament rounds
    attacker_tournament_team UUID NULL,              -- raw ObjectID UUID
    attacker_hit_count       INT NOT NULL,
    defender_country_id      INT NULL REFERENCES inventory_ids(id),  -- NULL on tournament rounds
    defender_tournament_team UUID NULL,              -- raw ObjectID UUID
    defender_hit_count       INT NOT NULL,

    -- 2-byte aligned
    number                   SMALLINT NOT NULL,

    -- variable
    live                     JSONB NULL,    -- { ticksCount, actualTickPoints, nextTickAt }

    -- surrogate int PK; referenced by ranking entries
    id                       SERIAL PRIMARY KEY
);

-- round number is unique within a battle; anchors round_number references
-- from round ranking entries
ALTER TABLE rounds ADD CONSTRAINT rounds_battle_number UNIQUE (battle_id, number);

CREATE TABLE battle_bounties (
    -- 8-byte aligned
    money_pool           DOUBLE PRECISION NULL,
    money_per_1k_damages DOUBLE PRECISION NULL,
    bounty_effective_at  TIMESTAMPTZ NULL,   -- bounty claimable time, NOT battle creation

    -- 4-byte aligned
    battle_id            UUID NOT NULL REFERENCES battles(battle_id),

    -- 2-byte aligned
    side                 SMALLINT NOT NULL,   -- 1 = attacker, 2 = defender
    bounty_is_national   BOOLEAN NULL,

    UNIQUE (battle_id, side)   -- a row exists only when the side has a bounty value
);


-- =============================================================================
-- 5. Countries (current-state detail)
--
-- inventory_ids stays the global id map (every country/region/MU/user id);
-- `countries` adds current-state detail keyed on the SAME id
-- (country_id INT PRIMARY KEY == inventory_ids.id), so all existing FKs
-- (battles.*, rounds.*, transactions.*) keep pointing at inventory_ids.
--
-- Source: country.getAllCountries (1 request, ~180 docs). Values are a
-- point-in-time snapshot refreshed on every load — no history kept (the
-- fields only matter for current-day gameplay decisions). `name` can change
-- (revolutions rename countries) — code is the stable 2-letter key.
-- Field reality: currentPopulation INT always present; core/average/current
-- Development DOUBLE always present; income/market/selfWork taxes DOUBLE %
-- (fractional on ~22/2/45 of 180 countries); productionPercent % — NULL on
-- ~72 countries without strategic resources; specializedItem — NULL on ~27.
-- =============================================================================

CREATE TABLE countries (
    -- 8-byte aligned
    updated_at            TIMESTAMPTZ NOT NULL,       -- our fetch time (refreshed each load)
    core_development      DOUBLE PRECISION NOT NULL,
    average_development   DOUBLE PRECISION NOT NULL,
    current_development   DOUBLE PRECISION NOT NULL,
    income_tax            DOUBLE PRECISION NULL,      -- % — fractional on 22/180 countries
    market_tax            DOUBLE PRECISION NULL,      -- % — fractional on 2/180 countries
    self_work_tax         DOUBLE PRECISION NULL,      -- % — fractional on 45/180 countries

    -- 4-byte aligned
    country_id            INT PRIMARY KEY REFERENCES inventory_ids(id),
    current_population    INT NOT NULL,
    production_percent    DOUBLE PRECISION NULL,      -- % — NULL when no strategic resources

    -- text
    name                  TEXT NOT NULL,
    code                  TEXT NOT NULL UNIQUE,       -- 2-letter, stable across renames
    specialized_item      TEXT NULL
);


-- =============================================================================
-- 6. Battle ranking entries (ranking data + item loot)
--
-- Two hypertables: battle-level and round-level rankings. One row per
-- (battle/round, side, entity type, entity) with damage/points/money merged
-- into a single row (nullable — a user may appear in only some dataTypes).
-- Loot items are referenced via items(id) (upserted through get_item_id());
-- loot is duplicated per side exactly as the API returns it (both-sides
-- users carry the same item on attacker/defender/merged rows).
-- No rank columns (derived via RANK() OVER at query time), no updated_at
-- (entries are immutable once written at battle end).
-- Source: battleRanking.getRanking (27 battle combos + 27 per round:
-- 3 dataTypes × 3 types × 3 sides; round rankings queried with roundId ONLY).
--
-- Hypertables partitioned by created_at (7-day chunks) with native
-- TimescaleDB compression: heap ~5-7x smaller; per-chunk indexes drop off
-- compressed chunks (queries on old data use the segment metadata instead).
-- The unique key MUST include the partition column → the upsert ON CONFLICT
-- targets in insert_battle_ranking_entry / insert_round_ranking_entry match
-- (created_at, battle_id, ...). Upserts and DELETEs work against compressed
-- chunks; the compression policies below keep recent chunks uncompressed so
-- the battle-end fetch can write final rows.
-- =============================================================================

CREATE TABLE battle_ranking_entries (
    -- 8-byte aligned
    damage         BIGINT NULL,        -- API damage (integer values)
    money          DOUBLE PRECISION NULL,  -- always fractional
    loot_item_id   BIGINT NULL REFERENCES items(id),  -- NULL = no loot / battle active
    created_at     TIMESTAMPTZ NOT NULL,   -- ranking entry createdAt (hypertable partition col)

    -- 4-byte aligned
    battle_id      INT NOT NULL REFERENCES battles(id),   -- serial PK, not the UUID
    entity_id      INT NOT NULL REFERENCES inventory_ids(id),  -- user/country/mu id (same map)
    points         INT NULL,           -- normal int, not smallint (countries grow)

    -- 2-byte aligned
    side           SMALLINT NOT NULL,  -- 1 = attacker, 2 = defender, 3 = merged (exceptions only)
    entity_type    SMALLINT NOT NULL   -- 1 = user, 2 = country, 3 = mu
);

SELECT create_hypertable('battle_ranking_entries', 'created_at',
                         chunk_time_interval => INTERVAL '7 days');
-- TimescaleDB auto-creates an index on the partition column; redundant here
-- (the unique key below starts with created_at).
DROP INDEX IF EXISTS battle_ranking_entries_created_at_idx;
ALTER TABLE battle_ranking_entries SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'side, entity_type',
    timescaledb.compress_orderby = 'battle_id, created_at');
CREATE UNIQUE INDEX battle_ranking_entries_pkey
    ON battle_ranking_entries (created_at, battle_id, side, entity_type, entity_id);

CREATE TABLE round_ranking_entries (
    -- 8-byte aligned
    damage         BIGINT NULL,
    money          DOUBLE PRECISION NULL,
    loot_item_id   BIGINT NULL REFERENCES items(id),
    created_at     TIMESTAMPTZ NOT NULL,

    -- 4-byte aligned
    battle_id      INT NOT NULL REFERENCES battles(id),
    entity_id      INT NOT NULL REFERENCES inventory_ids(id),
    points         INT NULL,

    -- 2-byte aligned
    round_number   SMALLINT NOT NULL,  -- 1-3, unique per battle (rounds UNIQUE(battle_id, number))
    side           SMALLINT NOT NULL,
    entity_type    SMALLINT NOT NULL
);

SELECT create_hypertable('round_ranking_entries', 'created_at',
                         chunk_time_interval => INTERVAL '7 days');
-- See the battle table above: drop the redundant partition-column index.
DROP INDEX IF EXISTS round_ranking_entries_created_at_idx;
ALTER TABLE round_ranking_entries SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'side, entity_type',
    timescaledb.compress_orderby = 'battle_id, created_at');
CREATE UNIQUE INDEX round_ranking_entries_pkey
    ON round_ranking_entries (created_at, battle_id, round_number, side, entity_type, entity_id);

-- Compress chunks older than 7 days (12 h schedule; recent chunks stay
-- uncompressed for the live battle-end fetch)
SELECT add_compression_policy('battle_ranking_entries', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('round_ranking_entries', INTERVAL '7 days', if_not_exists => TRUE);


-- =============================================================================
-- 8. Endpoint usage tracking
--
-- `endpoints` is the registry of known API endpoints (seeded from
-- extra/endpoints.json, auto-extended at runtime); `endpoints_used` is an
-- append-only log — one row per API call the scripts make. Rows accumulate
-- (bounded by how often the pipeline runs; ~200k/day at the 15s web cycle).
-- =============================================================================

CREATE TABLE endpoints (
    id   SMALLSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL   -- 'country.getCountryById', 'battle.getBattles', ...
);

CREATE TABLE endpoints_used (
    id          BIGSERIAL PRIMARY KEY,
    endpoint_id SMALLINT NOT NULL REFERENCES endpoints(id),
    date_used   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- =============================================================================
-- 8. Users
--
-- One row per user (rankings entity_type=1 ∪ transaction roles ∪
-- active-leaderboard users). user_damages/user_bounty = API lifetime stats
-- where ranking.getRanking snapshots cover the user (update_users.py
-- overwrites them), else derived Σ merged battle sums (initial-load backfill).
-- created_at is NOT stored: derivable from the user ObjectID timestamp
-- (which is the game restart 2025-05-01 for old users, NOT account creation).
-- =============================================================================

CREATE TABLE users (
    id              SERIAL PRIMARY KEY,                       -- 4B int PK
    user_id         UUID NOT NULL UNIQUE REFERENCES inventory_ids(external_id),
    user_damages    BIGINT NOT NULL DEFAULT 0,                -- Σ merged battle-level damage (rankings)
    user_wealth     DOUBLE PRECISION NULL,                    -- getUserLite rankings.userWealth.value
    user_bounty     DOUBLE PRECISION NOT NULL DEFAULT 0,      -- Σ merged battle-level money (rankings)
    mu_id           INT NULL REFERENCES inventory_ids(id),    -- user's MU (getUserLite mu)
    total_xp        INTEGER NULL,                             -- getUserLite leveling.totalXp
    military_rank   SMALLINT NULL,                            -- getUserLite militaryRank (numeric)
    username        TEXT NULL,                                -- getUserLite username
    lite_checked_at TIMESTAMPTZ NULL                          -- last successful getUserLite fetch
);
