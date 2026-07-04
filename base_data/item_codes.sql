INSERT INTO item_codes (code) VALUES 
    -- Equipment Items
    ('knife'), ('gun'), ('rifle'), ('sniper'), ('tank'), ('jet'),

    ('helmet1'), ('helmet2'), ('helmet3'), ('helmet4'), ('helmet5'), ('helmet6'),
    ('chest1'),  ('chest2'),  ('chest3'),  ('chest4'),  ('chest5'),  ('chest6') ,
    ('pants1'),  ('pants2'),  ('pants3'),  ('pants4'),  ('pants5'),  ('pants6') ,
    ('gloves1'), ('gloves2'), ('gloves3'), ('gloves4'), ('gloves5'), ('gloves6'),
    ('boots1'),  ('boots2'),  ('boots3'),  ('boots4'),  ('boots5'),  ('boots6')
    
    -- Market items
    

    ON CONFLICT (code) DO NOTHING;
    ;