-- =============================================
-- 0. Conversion helpers (TEXT ObjectID ↔ UUID)
-- =============================================

CREATE OR REPLACE FUNCTION objectid_to_uuid(hex TEXT) RETURNS UUID AS $$
BEGIN
    IF hex IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN (hex || '00000000')::UUID;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION uuid_to_objectid(u UUID) RETURNS TEXT AS $$
BEGIN
    IF u IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN LOWER(LEFT(REPLACE(u::TEXT, '-', ''), 24));
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- =============================================
-- 1. Helper functions for ID resolution
-- =============================================

CREATE OR REPLACE FUNCTION get_inventory_id(p_external_id TEXT)
RETURNS INT AS $$
DECLARE
    v_id INT;
BEGIN
    IF p_external_id IS NULL THEN
        RETURN NULL;
    END IF;
    -- SELECT-first: the sequence (SERIAL) must only advance for genuinely new
    -- entities. INSERT ... ON CONFLICT burns a nextval() on EVERY call even
    -- when the row already exists — with 70M transactions × ~4 lookups that
    -- overflows the sequences. The INSERT below stays only as a race-safety
    -- net (a burn happens only on a true concurrent miss for a new id).
    SELECT id INTO v_id FROM inventory_ids WHERE external_id = objectid_to_uuid(p_external_id);
    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;
    INSERT INTO inventory_ids (external_id)
    VALUES (objectid_to_uuid(p_external_id))
    ON CONFLICT (external_id) DO NOTHING
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        SELECT id INTO v_id FROM inventory_ids WHERE external_id = objectid_to_uuid(p_external_id);
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_item_code_id(p_code TEXT)
RETURNS SMALLINT AS $$
DECLARE
    v_id SMALLINT;
BEGIN
    IF p_code IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT id INTO v_id FROM item_codes WHERE code = p_code;
    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;
    INSERT INTO item_codes (code)
    VALUES (p_code)
    ON CONFLICT (code) DO NOTHING
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        SELECT id INTO v_id FROM item_codes WHERE code = p_code;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_transaction_type_id(p_type TEXT)
RETURNS SMALLINT AS $$
DECLARE
    v_id SMALLINT;
BEGIN
    IF p_type IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT id INTO v_id FROM transaction_types WHERE type = p_type;
    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;
    INSERT INTO transaction_types (type)
    VALUES (p_type)
    ON CONFLICT (type) DO NOTHING
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        SELECT id INTO v_id FROM transaction_types WHERE type = p_type;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- 1.5. Item resolver (insert-or-get by MongoDB UUID)
-- =============================================

CREATE OR REPLACE FUNCTION get_item_id(
    p_item_uuid TEXT,
    p_item_code_id SMALLINT,
    p_primary_skill SMALLINT,
    p_secondary_skill SMALLINT,
    p_last_acquisition_at TIMESTAMPTZ DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_id BIGINT;
BEGIN
    IF p_item_uuid IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT id INTO v_id FROM items WHERE item_uuid = objectid_to_uuid(p_item_uuid);
    IF v_id IS NOT NULL THEN
        -- existing item: only refresh last_acquisition_at (no sequence burn)
        IF p_last_acquisition_at IS NOT NULL THEN
            UPDATE items SET last_acquisition_at = CASE
                WHEN items.last_acquisition_at IS NULL THEN p_last_acquisition_at
                ELSE GREATEST(items.last_acquisition_at, p_last_acquisition_at)
            END
            WHERE id = v_id;
        END IF;
        RETURN v_id;
    END IF;
    INSERT INTO items (item_uuid, item_code_id, primary_skill, secondary_skill, last_acquisition_at)
    VALUES (objectid_to_uuid(p_item_uuid), p_item_code_id, p_primary_skill, p_secondary_skill, p_last_acquisition_at)
    ON CONFLICT (item_uuid) DO UPDATE SET
        last_acquisition_at = CASE
            WHEN EXCLUDED.last_acquisition_at IS NULL THEN items.last_acquisition_at
            WHEN items.last_acquisition_at IS NULL THEN EXCLUDED.last_acquisition_at
            ELSE GREATEST(items.last_acquisition_at, EXCLUDED.last_acquisition_at)
        END
    RETURNING id INTO v_id;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- 2. Skill extraction helper
-- =============================================

CREATE OR REPLACE FUNCTION extract_skills(p_item JSONB, p_code TEXT)
RETURNS TABLE(primary_skill SMALLINT, secondary_skill SMALLINT) AS $$
DECLARE
    v_skills JSONB;
    v_attack SMALLINT;
    v_crit SMALLINT;
    v_other SMALLINT;
BEGIN
    IF p_item IS NULL OR p_item->'skills' IS NULL THEN
        primary_skill := NULL;
        secondary_skill := NULL;
        RETURN NEXT;
        RETURN;
    END IF;
    
    v_skills := p_item->'skills';
    
    -- Weapons: have 'attack' and 'criticalChance'
    IF p_code IN ('knife','gun','rifle','sniper','tank','jet') THEN
        v_attack := (v_skills->>'attack')::SMALLINT;
        v_crit := (v_skills->>'criticalChance')::SMALLINT;
        primary_skill := v_attack;
        secondary_skill := v_crit;
    ELSE
        -- Equipment: single skill – take the first numeric value
        SELECT (value)::SMALLINT INTO v_other
        FROM jsonb_each(v_skills)
        WHERE jsonb_typeof(value) = 'number'
        LIMIT 1;
        primary_skill := v_other;
        secondary_skill := NULL;
    END IF;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- 3. Main insertion function
-- =============================================

CREATE OR REPLACE FUNCTION insert_transaction(payload JSONB)
RETURNS BIGINT AS $$
DECLARE
    v_transaction_id UUID;
    v_created_at TIMESTAMPTZ;
    v_offer_created_at TIMESTAMPTZ;
    v_seller_id INT;
    v_buyer_id INT;
    v_secondary_seller_id INT;
    v_secondary_buyer_id INT;
    v_item_code_id SMALLINT;
    v_transaction_type_id SMALLINT;
    v_money DOUBLE PRECISION;
    v_quantity DOUBLE PRECISION;
    v_primary_skill SMALLINT;
    v_secondary_skill SMALLINT;
    v_item JSONB;
    v_skill_code TEXT;
    v_item_id BIGINT;
    v_item_uuid TEXT;
    v_last_acquisition_at TIMESTAMPTZ;
BEGIN
    -- 1. Extract basic fields and convert ObjectID hex to UUID
    v_transaction_id := objectid_to_uuid(payload->>'_id');
    v_created_at := (payload->>'createdAt')::TIMESTAMPTZ;
    v_offer_created_at := (payload->>'offerCreatedAt')::TIMESTAMPTZ;
    v_money := (payload->>'money')::DOUBLE PRECISION;
    v_quantity := (payload->>'quantity')::DOUBLE PRECISION;

    -- 2. Resolve IDs
    v_seller_id := get_inventory_id(payload->>'sellerId');
    v_buyer_id := get_inventory_id(payload->>'buyerId');

    IF payload ? 'sellerMuId' THEN
        v_secondary_seller_id := get_inventory_id(payload->>'sellerMuId');
    ELSIF payload ? 'sellerCountryId' THEN
        v_secondary_seller_id := get_inventory_id(payload->>'sellerCountryId');
    ELSE
        v_secondary_seller_id := NULL;
    END IF;

    IF payload ? 'buyerMuId' THEN
        v_secondary_buyer_id := get_inventory_id(payload->>'buyerMuId');
    ELSIF payload ? 'buyerCountryId' THEN
        v_secondary_buyer_id := get_inventory_id(payload->>'buyerCountryId');
    ELSE
        v_secondary_buyer_id := NULL;
    END IF;

    -- 3. Item codes and item instance resolution
    -- item_code_id stores what was traded / the case / the input material.
    v_item_code_id := get_item_code_id(payload->>'itemCode');
    
    v_item := payload->'item';
    -- Use the result item code for skill classification when available
    -- (openCase / craftItem / dismantleItem produce items whose code
    --  differs from the outer itemCode, and the skills belong to the result)
    v_skill_code := COALESCE(payload->>'resultItemCode', payload->>'itemCode');
    SELECT * INTO v_primary_skill, v_secondary_skill
    FROM extract_skills(v_item, v_skill_code);
    
    -- Resolve the item instance (create items row if first sighting)
    v_item_uuid := v_item->>'_id';
    v_last_acquisition_at := (v_item->>'lastAcquisitionAt')::TIMESTAMPTZ;
    v_item_id := get_item_id(
        v_item_uuid,
        get_item_code_id(v_skill_code),
        v_primary_skill,
        v_secondary_skill,
        v_last_acquisition_at
    );

    -- 4. Transaction type (auto-inserts unknown types via get_transaction_type_id)
    v_transaction_type_id := get_transaction_type_id(payload->>'transactionType');

    -- 5. Insert (skip silently if the transaction_id already exists)
    INSERT INTO transactions (
        transaction_id,
        created_at,
        offer_created_at,
        seller_id,
        buyer_id,
        secondary_seller_id,
        secondary_buyer_id,
        item_code_id,
        item_id,
        transaction_type_id,
        money,
        quantity
    ) VALUES (
        v_transaction_id,
        v_created_at,
        v_offer_created_at,
        v_seller_id,
        v_buyer_id,
        v_secondary_seller_id,
        v_secondary_buyer_id,
        v_item_code_id,
        v_item_id,
        v_transaction_type_id,
        v_money,
        v_quantity
    )
    ON CONFLICT (transaction_id, created_at) DO NOTHING;

    IF FOUND THEN
        RETURN 1;
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- 4. Ensure all transaction types exist (run once)
-- =============================================

INSERT INTO transaction_types (type) VALUES
    ('applicationFee'), ('trading'), ('itemMarket'), ('wage'),
    ('donation'), ('articleTip'), ('openCase'), ('craftItem'),
    ('dismantleItem'), ('battleLoot'), ('countryMoneyTransfer')
ON CONFLICT (type) DO NOTHING;


-- =============================================
-- 4. Battle functions (Phase 1 of the battle expansion)
--
-- Idempotent: ON CONFLICT DO NOTHING, safe to re-run on the cache files.
-- Payloads come from extra/battles_cache/ (battles.jsonl.gz = full battle
-- docs minus currentRound; rounds.jsonl.gz = minimized round docs).
-- =============================================

CREATE OR REPLACE FUNCTION get_battle_type_id(p_type TEXT)
RETURNS SMALLINT AS $$
DECLARE
    v_id SMALLINT;
BEGIN
    IF p_type IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT id INTO v_id FROM battle_types WHERE code = p_type;
    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;
    INSERT INTO battle_types (code)
    VALUES (p_type)
    ON CONFLICT (code) DO NOTHING
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        SELECT id INTO v_id FROM battle_types WHERE code = p_type;
    END IF;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_battle(payload JSONB)
RETURNS UUID AS $$
DECLARE
    v_battle_id UUID;
    v_type_id SMALLINT;
    v_attacker JSONB;
    v_defender JSONB;
    v_side JSONB;
    v_side_no SMALLINT;
    v_inserted UUID;
BEGIN
    v_battle_id := objectid_to_uuid(payload->>'_id');
    v_type_id := get_battle_type_id(payload->>'type');
    v_attacker := payload->'attacker';
    v_defender := payload->'defender';

    INSERT INTO battles (
        battle_id, created_at, updated_at, ended_at,
        type_id,
        war_id, tournament_id, tournament_round_number,
        revolution_party_id, revolution_processed, is_system_resistance,
        is_big_battle,
        attacker_country_id, attacker_region, attacker_tournament_team,
        attacker_damages, attacker_hit_count, attacker_won_rounds_count,
        defender_country_id, defender_region, defender_tournament_team,
        defender_damages, defender_hit_count, defender_won_rounds_count
    )
    VALUES (
        v_battle_id,
        (payload->>'createdAt')::TIMESTAMPTZ,
        (payload->>'updatedAt')::TIMESTAMPTZ,
        (payload->>'endedAt')::TIMESTAMPTZ,
        v_type_id,
        objectid_to_uuid(payload->>'war'),
        objectid_to_uuid(payload->>'tournament'),
        (payload->>'tournamentRoundNumber')::SMALLINT,
        get_inventory_id(payload->>'revolutionParty'),
        (payload->>'revolutionProcessed')::BOOLEAN,
        (payload->>'isSystemResistance')::BOOLEAN,
        (payload->>'isBigBattle')::BOOLEAN,
        get_inventory_id(v_attacker->>'country'),
        objectid_to_uuid(v_attacker->>'region'),
        objectid_to_uuid(v_attacker->>'tournamentTeam'),
        (v_attacker->>'damages')::DOUBLE PRECISION,
        (v_attacker->>'hitCount')::INTEGER,
        (v_attacker->>'wonRoundsCount')::SMALLINT,
        get_inventory_id(v_defender->>'country'),
        objectid_to_uuid(v_defender->>'region'),
        objectid_to_uuid(v_defender->>'tournamentTeam'),
        (v_defender->>'damages')::DOUBLE PRECISION,
        (v_defender->>'hitCount')::INTEGER,
        (v_defender->>'wonRoundsCount')::SMALLINT
    )
    ON CONFLICT (battle_id) DO UPDATE SET
        -- re-fetches of active battles (update_battles.py) refresh mutable stats
        updated_at = EXCLUDED.updated_at,
        ended_at = EXCLUDED.ended_at,
        attacker_damages = EXCLUDED.attacker_damages,
        attacker_hit_count = EXCLUDED.attacker_hit_count,
        attacker_won_rounds_count = EXCLUDED.attacker_won_rounds_count,
        defender_damages = EXCLUDED.defender_damages,
        defender_hit_count = EXCLUDED.defender_hit_count,
        defender_won_rounds_count = EXCLUDED.defender_won_rounds_count
    RETURNING battle_id INTO v_inserted;

    IF v_inserted IS NULL THEN
        SELECT battle_id INTO v_inserted FROM battles WHERE battle_id = v_battle_id;
    END IF;

    -- Per-side money/bounty fields (battle_bounties row only when a side
    -- actually has a value — pre-2025-07-14 battles have none)
    FOR v_side, v_side_no IN SELECT v_attacker, 1 UNION ALL SELECT v_defender, 2 LOOP
        IF (v_side->>'moneyPool') IS NOT NULL
           OR (v_side->>'moneyPer1kDamages') IS NOT NULL
           OR (v_side->>'bountyEffectiveAt') IS NOT NULL
           OR (v_side->>'bountyIsNational') IS NOT NULL
        THEN
            INSERT INTO battle_bounties (
                battle_id, side,
                money_pool, money_per_1k_damages, bounty_effective_at, bounty_is_national
            )
            VALUES (
                v_battle_id, v_side_no,
                (v_side->>'moneyPool')::DOUBLE PRECISION,
                (v_side->>'moneyPer1kDamages')::DOUBLE PRECISION,
                (v_side->>'bountyEffectiveAt')::TIMESTAMPTZ,
                (v_side->>'bountyIsNational')::BOOLEAN
            )
            ON CONFLICT (battle_id, side) DO NOTHING;
        END IF;
    END LOOP;

    RETURN v_inserted;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_round(payload JSONB)
RETURNS UUID AS $$
DECLARE
    v_round_id UUID;
    v_attacker JSONB;
    v_defender JSONB;
    v_winner_country_id INT;
    v_winner_team UUID;
    v_inserted UUID;
BEGIN
    v_round_id := objectid_to_uuid(payload->>'_id');
    v_attacker := payload->'attacker';
    v_defender := payload->'defender';

    -- wonBy format changed 2025-12: old rounds carry the winning country id,
    -- rounds since 2026-01 carry the side string 'attacker'/'defender'
    -- (verified 20,258 side-string rounds, 0 mismatches vs attacker/defender
    -- points). The winner is a country for war/resistance rounds, a team for
    -- tournament rounds.
    IF payload->>'wonBy' = 'attacker' THEN
        IF (v_attacker->>'tournamentTeam') IS NOT NULL THEN
            v_winner_team := objectid_to_uuid(v_attacker->>'tournamentTeam');
        ELSE
            v_winner_country_id := get_inventory_id(v_attacker->>'country');
        END IF;
    ELSIF payload->>'wonBy' = 'defender' THEN
        IF (v_defender->>'tournamentTeam') IS NOT NULL THEN
            v_winner_team := objectid_to_uuid(v_defender->>'tournamentTeam');
        ELSE
            v_winner_country_id := get_inventory_id(v_defender->>'country');
        END IF;
    ELSIF payload->>'wonBy' IS NOT NULL THEN
        v_winner_country_id := get_inventory_id(payload->>'wonBy');
    END IF;

    INSERT INTO rounds (
        round_id, battle_id, number,
        created_at, updated_at, ended_at,
        about_to_end_notified_at,
        won_by_country_id, won_by_tournament_team,
        live,
        attacker_country_id, attacker_tournament_team,
        attacker_damages, attacker_hit_count, attacker_points,
        defender_country_id, defender_tournament_team,
        defender_damages, defender_hit_count, defender_points
    )
    VALUES (
        v_round_id,
        objectid_to_uuid(payload->>'battle'),
        (payload->>'number')::SMALLINT,
        (payload->>'createdAt')::TIMESTAMPTZ,
        (payload->>'updatedAt')::TIMESTAMPTZ,
        (payload->>'endedAt')::TIMESTAMPTZ,
        (payload->>'aboutToEndNotifiedAt')::TIMESTAMPTZ,
        v_winner_country_id, v_winner_team,
        payload->'live',
        get_inventory_id(v_attacker->>'country'),
        objectid_to_uuid(v_attacker->>'tournamentTeam'),
        (v_attacker->>'damages')::DOUBLE PRECISION,
        COALESCE((v_attacker->>'hitCount')::INTEGER, 0),
        (v_attacker->>'points')::DOUBLE PRECISION,
        get_inventory_id(v_defender->>'country'),
        objectid_to_uuid(v_defender->>'tournamentTeam'),
        (v_defender->>'damages')::DOUBLE PRECISION,
        -- API quirk: 443 rounds (2025-05 → 2026-07, war/tournament/resistance)
        -- return defender.hitCount = null, always alongside damages=0/points=0
        COALESCE((v_defender->>'hitCount')::INTEGER, 0),
        (v_defender->>'points')::DOUBLE PRECISION
    )
    ON CONFLICT (round_id) DO UPDATE SET
        -- live rounds are re-fetched by update_battles.py; refresh mutable stats
        ended_at = EXCLUDED.ended_at,
        about_to_end_notified_at = EXCLUDED.about_to_end_notified_at,
        live = EXCLUDED.live,
        attacker_damages = EXCLUDED.attacker_damages,
        attacker_hit_count = EXCLUDED.attacker_hit_count,
        attacker_points = EXCLUDED.attacker_points,
        defender_damages = EXCLUDED.defender_damages,
        defender_hit_count = EXCLUDED.defender_hit_count,
        defender_points = EXCLUDED.defender_points
    RETURNING round_id INTO v_inserted;

    IF v_inserted IS NULL THEN
        SELECT round_id INTO v_inserted FROM rounds WHERE round_id = v_round_id;
    END IF;

    RETURN v_inserted;
END;
$$ LANGUAGE plpgsql;


-- =============================================
-- 5. Country functions (Phase 1.5 of the battle expansion)
--
-- Upsert keyed on country_id (== inventory_ids.id); re-running refreshes the
-- snapshot. payload = one doc from country.getAllCountries.
-- =============================================

CREATE OR REPLACE FUNCTION insert_country(payload JSONB)
RETURNS INT AS $$
DECLARE
    v_country_id INT;
BEGIN
    v_country_id := get_inventory_id(payload->>'_id');

    INSERT INTO countries (
        country_id, name, code,
        current_population,
        core_development, average_development, current_development,
        specialized_item, production_percent,
        income_tax, market_tax, self_work_tax,
        updated_at
    )
    VALUES (
        v_country_id,
        payload->>'name',
        payload->>'code',
        (payload->>'currentPopulation')::INTEGER,
        (payload->>'coreDevelopment')::DOUBLE PRECISION,
        (payload->>'averageDevelopment')::DOUBLE PRECISION,
        (payload->>'currentDevelopment')::DOUBLE PRECISION,
        payload->>'specializedItem',
        (payload->'strategicResources'->'bonuses'->>'productionPercent')::DOUBLE PRECISION,
        (payload->'taxes'->>'income')::DOUBLE PRECISION,
        (payload->'taxes'->>'market')::DOUBLE PRECISION,
        (payload->'taxes'->>'selfWork')::DOUBLE PRECISION,
        NOW()
    )
    ON CONFLICT (country_id) DO UPDATE SET
        name = EXCLUDED.name,
        current_population = EXCLUDED.current_population,
        core_development = EXCLUDED.core_development,
        average_development = EXCLUDED.average_development,
        current_development = EXCLUDED.current_development,
        specialized_item = EXCLUDED.specialized_item,
        production_percent = EXCLUDED.production_percent,
        income_tax = EXCLUDED.income_tax,
        market_tax = EXCLUDED.market_tax,
        self_work_tax = EXCLUDED.self_work_tax,
        updated_at = EXCLUDED.updated_at
    RETURNING country_id INTO v_country_id;

    RETURN v_country_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 7. Battle ranking entries (Phase 2 — rankings + loot)
--
-- One row per (battle/round, side, entity type, entity); damage/points/money
-- merged nullable; loot_item_id -> items(id); loot duplicated per side as the
-- API returns it. entity ids resolve through get_inventory_id() by the caller
-- (users/mus are mostly absent from inventory_ids until first seen).
-- See extra/battle_loot_db_plan.md.
-- =============================================================================

CREATE OR REPLACE FUNCTION insert_battle_ranking_entry(
    p_battle_hex TEXT,
    p_side SMALLINT,
    p_entity_type SMALLINT,
    p_entity_id INT,
    p_damage BIGINT,
    p_points INT,
    p_money DOUBLE PRECISION,
    p_loot_item_id BIGINT,
    p_created_at TIMESTAMPTZ
) RETURNS VOID AS $$
BEGIN
    INSERT INTO battle_ranking_entries (
        battle_id, side, entity_type, entity_id,
        damage, points, money, loot_item_id, created_at
    )
    SELECT b.id, p_side, p_entity_type, p_entity_id,
           p_damage, p_points, p_money, p_loot_item_id, p_created_at
    FROM battles b WHERE b.battle_id = objectid_to_uuid(p_battle_hex)
    ON CONFLICT (battle_id, side, entity_type, entity_id) DO UPDATE SET
        damage = EXCLUDED.damage,
        points = EXCLUDED.points,
        money = EXCLUDED.money,
        loot_item_id = EXCLUDED.loot_item_id,
        created_at = EXCLUDED.created_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_round_ranking_entry(
    p_battle_hex TEXT,
    p_round_number SMALLINT,
    p_side SMALLINT,
    p_entity_type SMALLINT,
    p_entity_id INT,
    p_damage BIGINT,
    p_points INT,
    p_money DOUBLE PRECISION,
    p_loot_item_id BIGINT,
    p_created_at TIMESTAMPTZ
) RETURNS VOID AS $$
BEGIN
    INSERT INTO round_ranking_entries (
        battle_id, round_number, side, entity_type, entity_id,
        damage, points, money, loot_item_id, created_at
    )
    SELECT b.id, p_round_number, p_side, p_entity_type, p_entity_id,
           p_damage, p_points, p_money, p_loot_item_id, p_created_at
    FROM battles b WHERE b.battle_id = objectid_to_uuid(p_battle_hex)
    ON CONFLICT (battle_id, round_number, side, entity_type, entity_id) DO UPDATE SET
        damage = EXCLUDED.damage,
        points = EXCLUDED.points,
        money = EXCLUDED.money,
        loot_item_id = EXCLUDED.loot_item_id,
        created_at = EXCLUDED.created_at;
END;
$$ LANGUAGE plpgsql;
