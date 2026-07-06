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
