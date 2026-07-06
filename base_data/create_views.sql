-- =============================================
-- 1. Readable transaction view
--
-- Resolves the integer FK columns to their
-- human-readable string equivalents so you can
-- query without mentally mapping IDs to codes.
-- =============================================

CREATE OR REPLACE VIEW transaction_details AS
SELECT
    t.id,
    t.transaction_id,
    t.created_at,
    t.offer_created_at,

    -- Inventory references (UUID strings)
    t.seller_id,
    t.buyer_id,
    t.secondary_seller_id,
    t.secondary_buyer_id,

    -- Item codes resolved from the lookup table
    ic.code  AS item_code,
    rc.code  AS result_item_code,

    -- Transaction type
    tt.type  AS transaction_type,

    -- Numeric payload
    t.money,
    t.quantity,

    -- Skill breakdown
    t.primary_skill,
    t.secondary_skill,

    -- Catch-all
    t.extra

FROM transactions t
LEFT JOIN item_codes        ic  ON t.item_code_id        = ic.id
LEFT JOIN item_codes        rc  ON t.result_item_code_id = rc.id
JOIN   transaction_types    tt  ON t.transaction_type_id = tt.id;
