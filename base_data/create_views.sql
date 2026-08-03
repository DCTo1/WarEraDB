-- =============================================
-- 1. Readable transaction view
--
-- Resolves the integer FK columns to their
-- human-readable string equivalents so you can
-- query without mentally mapping IDs to codes.
-- =============================================

CREATE OR REPLACE VIEW transaction_details AS
SELECT
    t.transaction_id,
    t.created_at,
    t.offer_created_at,

    -- Inventory references (UUID strings)
    t.seller_id,
    t.buyer_id,
    t.secondary_seller_id,
    t.secondary_buyer_id,

    -- Item code (what was traded / the case / the input material)
    ic.code  AS item_code,

    -- Resolved item instance details (code + skills from items table)
    itm.code  AS item_instance_code,
    i.primary_skill,
    i.secondary_skill,
    i.first_seen_at,
    i.last_acquisition_at,

    -- Transaction type
    tt.type  AS transaction_type,

    -- Numeric payload
    t.money,
    t.quantity

FROM transactions t
LEFT JOIN item_codes        ic  ON t.item_code_id = ic.id
LEFT JOIN items             i   ON t.item_id      = i.id
LEFT JOIN item_codes        itm ON i.item_code_id = itm.id
JOIN   transaction_types    tt  ON t.transaction_type_id = tt.id;


-- =============================================
-- 2. Battle views (Phase 1 of the battle expansion)
--
-- Resolve FK columns to readable ObjectID strings and join the per-side
-- bounty rows. Derived fields (wonBy, roundsToWin, isActive) are NOT stored
-- and NOT exposed: wonBy = side with higher won_rounds_count, active ⇔
-- ended_at IS NULL (see extra/deprecated/battle_db_expansion_plan.md).
-- =============================================

CREATE OR REPLACE VIEW battle_details AS
SELECT
    b.battle_id,
    b.created_at,
    b.ended_at,
    bt.code AS battle_type,

    -- Resolved references (ObjectID hex strings)
    b.war_id,
    b.tournament_id,
    b.tournament_round_number,
    b.revolution_party_id,
    b.is_big_battle,
    uuid_to_objectid(ac.external_id) AS attacker_country,
    c1.name AS attacker_country_name,
    uuid_to_objectid(b.attacker_region) AS attacker_region,
    uuid_to_objectid(b.attacker_tournament_team) AS attacker_tournament_team,
    b.attacker_damages,
    b.attacker_hit_count,
    b.attacker_won_rounds_count,
    uuid_to_objectid(dc.external_id) AS defender_country,
    c2.name AS defender_country_name,
    uuid_to_objectid(b.defender_region) AS defender_region,
    uuid_to_objectid(b.defender_tournament_team) AS defender_tournament_team,
    b.defender_damages,
    b.defender_hit_count,
    b.defender_won_rounds_count,

    -- Bounties (per side)
    ab.money_pool           AS attacker_money_pool,
    ab.money_per_1k_damages AS attacker_money_per_1k_damages,
    ab.bounty_effective_at  AS attacker_bounty_effective_at,
    ab.bounty_is_national   AS attacker_bounty_is_national,
    db.money_pool           AS defender_money_pool,
    db.money_per_1k_damages AS defender_money_per_1k_damages,
    db.bounty_effective_at  AS defender_bounty_effective_at,
    db.bounty_is_national   AS defender_bounty_is_national

FROM battles b
JOIN battle_types bt ON bt.id = b.type_id
LEFT JOIN inventory_ids ac ON ac.id = b.attacker_country_id
LEFT JOIN countries c1 ON c1.country_id = b.attacker_country_id
LEFT JOIN inventory_ids dc ON dc.id = b.defender_country_id
LEFT JOIN countries c2 ON c2.country_id = b.defender_country_id
LEFT JOIN battle_bounties ab ON ab.battle_id = b.battle_id AND ab.side = 1
LEFT JOIN battle_bounties db ON db.battle_id = b.battle_id AND db.side = 2;

CREATE OR REPLACE VIEW round_details AS
SELECT
    r.round_id,
    r.battle_id,
    r.number,
    r.created_at,
    r.ended_at,
    uuid_to_objectid(wc.external_id) AS won_by_country,
    c3.name AS won_by_country_name,
    uuid_to_objectid(r.won_by_tournament_team) AS won_by_tournament_team,
    r.about_to_end_notified_at,
    uuid_to_objectid(ac.external_id) AS attacker_country,
    uuid_to_objectid(r.attacker_tournament_team) AS attacker_tournament_team,
    r.attacker_damages,
    r.attacker_hit_count,
    r.attacker_points,
    uuid_to_objectid(dc.external_id) AS defender_country,
    uuid_to_objectid(r.defender_tournament_team) AS defender_tournament_team,
    r.defender_damages,
    r.defender_hit_count,
    r.defender_points,
    r.live

FROM rounds r
LEFT JOIN inventory_ids wc ON wc.id = r.won_by_country_id
LEFT JOIN countries c3 ON c3.country_id = r.won_by_country_id
LEFT JOIN inventory_ids ac ON ac.id = r.attacker_country_id
LEFT JOIN inventory_ids dc ON dc.id = r.defender_country_id;


-- =============================================
-- 3. Battle bounty view
--
-- Battle_details filtered to battles with at least one bounty side
-- (battle_bounties row exists). Views are auto-inlined, so WHERE filters
-- on this view push down to the underlying joins — e.g.
--   "all bounties of a country":
--   SELECT * FROM battle_bounty_details
--   WHERE 'Germany' IN (attacker_country_name, defender_country_name);
--   -- or national bounties only:
--   WHERE attacker_bounty_is_national OR defender_bounty_is_national
-- bounty_side_count = 2 for battles where BOTH sides carry a bounty.
-- =============================================

CREATE OR REPLACE VIEW battle_bounty_details AS
SELECT
    *,
    (attacker_money_pool IS NOT NULL)::INT + (defender_money_pool IS NOT NULL)::INT
        AS bounty_side_count
FROM battle_details
WHERE attacker_money_pool IS NOT NULL
   OR defender_money_pool IS NOT NULL;


-- =============================================
-- 4. Bounty money per country
--
-- One row per country that carries at least one battle bounty (attacker or
-- defender side). total_pool = all money stored in its bounty pools;
-- ended_battles_pool = money in bounty pools of battles that already ended
-- (battles.ended_at IS NOT NULL — a battle is active ⇔ ended_at IS NULL).
-- The game lets players park money in ended battles' bounty pools as
-- untaxed storage, so a country where ended_battles_pool ≈ total_pool is
-- "hiding" most of its bounty wealth. money_pool can be negative (refunds),
-- sums reflect that.
-- =============================================

CREATE OR REPLACE VIEW country_bounty_summary AS
SELECT
    c.country_id,
    c.name AS country,
    SUM(x.money_pool) AS total_pool,
    SUM(x.money_pool) FILTER (WHERE x.ended_at IS NOT NULL) AS ended_battles_pool,
    COUNT(*) AS bounty_battle_sides
FROM countries c
JOIN (
    SELECT bb.money_pool, b.attacker_country_id AS country_id, b.ended_at
    FROM battle_bounties bb JOIN battles b ON b.battle_id = bb.battle_id
    WHERE bb.side = 1
    UNION ALL
    SELECT bb.money_pool, b.defender_country_id AS country_id, b.ended_at
    FROM battle_bounties bb JOIN battles b ON b.battle_id = bb.battle_id
    WHERE bb.side = 2
) x ON x.country_id = c.country_id
GROUP BY c.country_id, c.name;
