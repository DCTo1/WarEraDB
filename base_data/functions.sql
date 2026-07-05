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
    VALUES (p_external_id)
    ON CONFLICT (external_id) DO NOTHING
    RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        SELECT id INTO v_id FROM inventory_ids WHERE external_id = p_external_id;
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
    v_transaction_id TEXT;
    v_created_at TIMESTAMPTZ;
    v_offer_created_at TIMESTAMPTZ;
    v_seller_id INT;
    v_buyer_id INT;
    v_secondary_seller_id INT;
    v_secondary_buyer_id INT;
    v_item_code_id SMALLINT;
    v_result_item_code_id SMALLINT;
    v_transaction_type_id SMALLINT;
    v_money DOUBLE PRECISION;
    v_quantity DOUBLE PRECISION;
    v_primary_skill SMALLINT;
    v_secondary_skill SMALLINT;
    v_extra JSONB;
    v_item JSONB;
    v_skill_code TEXT;
    v_new_id BIGINT;
BEGIN
    -- 1. Extract basic fields
    v_transaction_id := payload->>'_id';
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

    -- 3. Item code, result item code, and skills
    v_item_code_id := get_item_code_id(payload->>'itemCode');
    v_result_item_code_id := get_item_code_id(payload->>'resultItemCode');
    
    v_item := payload->'item';
    -- Use the result item code for skill classification when available
    -- (openCase / craftItem / dismantleItem produce items whose code
    --  differs from the outer itemCode, and the skills belong to the result)
    v_skill_code := COALESCE(payload->>'resultItemCode', payload->>'itemCode');
    SELECT * INTO v_primary_skill, v_secondary_skill
    FROM extract_skills(v_item, v_skill_code);

    -- 4. Transaction type (auto-inserts unknown types via get_transaction_type_id)
    v_transaction_type_id := get_transaction_type_id(payload->>'transactionType');

    -- 5. Build extra JSONB: remove all stored fields, then clean the nested 'item'
    v_extra := payload - ARRAY[
        '_id', '__v', 'updatedAt',
        'createdAt', 'offerCreatedAt',
        'transactionType', 'itemCode', 'resultItemCode',
        'money', 'quantity',
        'sellerId', 'buyerId',
        'sellerMuId', 'sellerCountryId',
        'buyerMuId', 'buyerCountryId'
    ];
    
    -- If there is an 'item' key, clean it (keep only _id and lastAcquisitionAt)
    IF v_extra ? 'item' THEN
        v_extra := jsonb_set(
            v_extra,
            '{item}',
            (v_extra->'item') - ARRAY['state', 'maxState', 'quantity', 'type', 'code', 'skills']
        );
        -- If after cleaning the item is empty, remove the key entirely
        IF v_extra->'item' = '{}'::JSONB OR v_extra->'item' IS NULL THEN
            v_extra := v_extra - 'item';
        END IF;
    END IF;

    -- If extra is empty or only nulls, set to NULL
    IF v_extra = '{}'::JSONB OR v_extra IS NULL THEN
        v_extra := NULL;
    END IF;

    -- 6. Insert
    INSERT INTO transactions (
        transaction_id,
        created_at,
        offer_created_at,
        seller_id,
        buyer_id,
        secondary_seller_id,
        secondary_buyer_id,
        item_code_id,
        result_item_code_id,
        transaction_type_id,
        money,
        quantity,
        primary_skill,
        secondary_skill,
        extra
    ) VALUES (
        v_transaction_id,
        v_created_at,
        v_offer_created_at,
        v_seller_id,
        v_buyer_id,
        v_secondary_seller_id,
        v_secondary_buyer_id,
        v_item_code_id,
        v_result_item_code_id,
        v_transaction_type_id,
        v_money,
        v_quantity,
        v_primary_skill,
        v_secondary_skill,
        v_extra
    )
    RETURNING id INTO v_new_id;

    RETURN v_new_id;
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
