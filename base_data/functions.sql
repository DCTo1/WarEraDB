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
    INSERT INTO items (item_uuid, item_code_id, primary_skill, secondary_skill, last_acquisition_at)
    VALUES (objectid_to_uuid(p_item_uuid), p_item_code_id, p_primary_skill, p_secondary_skill, p_last_acquisition_at)
    ON CONFLICT (item_uuid) DO UPDATE SET
        last_acquisition_at = CASE
            WHEN EXCLUDED.last_acquisition_at IS NULL THEN items.last_acquisition_at
            WHEN items.last_acquisition_at IS NULL THEN EXCLUDED.last_acquisition_at
            ELSE GREATEST(items.last_acquisition_at, EXCLUDED.last_acquisition_at)
        END
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        SELECT id INTO v_id FROM items WHERE item_uuid = objectid_to_uuid(p_item_uuid);
    END IF;
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
