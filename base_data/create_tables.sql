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


-- =============================================================================
-- 5. Battle tables (Phase 1 of the battle expansion)
--
-- Schema verified across 2025-05 → 2026-08 (15,791 battles + 33,170 rounds in
-- extra/battles_cache/). Design notes in extra/deprecated/battle_db_expansion_plan.md:
--   - no is_active column: ended_at IS NULL == active
--   - no won_by / badges_processed / stats / rounds_to_win: derived or dropped
--   - isResistance / isTournament covered by battle_types
--   - money/bounty fields live in battle_bounties (per side)
--   - tournament battles/rounds have tournamentTeam instead of country
--   - tournament roundsHistory inflation bug does NOT affect these tables
--     (rounds are stored per rounds[] id, not per roundsHistory entry)
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
    id                       SERIAL PRIMARY KEY,    -- surrogate int PK (migration 05); referenced by ranking entries / rounds
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
    is_big_battle            BOOLEAN NULL
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
    id                       SERIAL PRIMARY KEY,  -- surrogate int PK (migration 05); referenced by ranking entries
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
    live                     JSONB NULL    -- { ticksCount, actualTickPoints, nextTickAt }
);

-- round number is unique within a battle (data verified 33,220/33,220
-- distinct); anchors round_number references from round ranking entries
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
-- 6. Countries (detail table — Phase 1.5 of the battle expansion)
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
-- Field reality (checked 2026-08-02, 180/180 countries):
--   currentPopulation  INT 238..12190 (always present)
--   core/average/currentDevelopment DOUBLE (always present)
--   taxes income/market/selfWork DOUBLE % (fractional: 22/2/45 of 180 countries)
--   productionPercent DOUBLE % 5..30 — fractional on 8/108; MISSING on 72 (no resources)
--   specializedItem TEXT ≤10 chars — MISSING on 27 countries
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
    production_percent    DOUBLE PRECISION NULL,      -- % — fractional on 8/108 countries; NULL when no strategic resources

    -- text
    name                  TEXT NOT NULL,
    code                  TEXT NOT NULL UNIQUE,       -- 2-letter, stable across renames
    specialized_item      TEXT NULL
);

-- =============================================================================
-- 7. Battle ranking entries (ranking data + item loot — Phase 2 of battle
-- expansion; see extra/deprecated/battle_loot_db_plan.md)
--
-- Two tables: battle-level and round-level rankings. One row per
-- (battle/round, side, entity type, entity) with damage/points/money merged
-- into a single row (nullable — a user may appear in only some dataTypes).
-- Loot items are referenced via items(id) (upserted through get_item_id());
-- loot is duplicated per side exactly as the API returns it (both-sides
-- users carry the same item on attacker/defender/merged rows).
-- No rank columns (derived via RANK() OVER at query time), no updated_at
-- (entries are immutable once written at battle end).
-- Source: battleRanking.getRanking (27 battle combos + 27 per round:
-- 3 dataTypes × 3 types × 3 sides; round rankings queried with roundId ONLY).
-- =============================================================================

CREATE TABLE battle_ranking_entries (
    -- 8-byte aligned
    damage         BIGINT NULL,        -- always integer today; bigint for growth (max 91M today)
    money          DOUBLE PRECISION NULL,  -- always fractional
    loot_item_id   BIGINT NULL REFERENCES items(id),  -- NULL = no loot / battle active
    created_at     TIMESTAMPTZ NOT NULL,   -- ranking entry createdAt (hypertable partition col)

    -- 4-byte aligned
    battle_id      INT NOT NULL REFERENCES battles(id),   -- serial PK, not the UUID
    entity_id      INT NOT NULL REFERENCES inventory_ids(id),  -- user/country/mu id (same map)
    points         INT NULL,           -- normal int, not smallint (countries grow)

    -- 2-byte aligned
    side           SMALLINT NOT NULL,  -- 1 = attacker, 2 = defender, 3 = merged
    entity_type    SMALLINT NOT NULL,  -- 1 = user, 2 = country, 3 = mu — NOT merged (historical)

    PRIMARY KEY (battle_id, side, entity_type, entity_id)
);

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
    entity_type    SMALLINT NOT NULL,

    PRIMARY KEY (battle_id, round_number, side, entity_type, entity_id)
);

-- Per-battle lookups / battles-to-entries navigation
CREATE INDEX idx_battle_ranking_entries_battle ON battle_ranking_entries (battle_id);
CREATE INDEX idx_round_ranking_entries_battle ON round_ranking_entries (battle_id);
