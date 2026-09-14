-- Step 7: Type 1 upsert — load all distinct tags from raw staging data into dim_tag.
-- Type 1 means we only insert new tags; no history is kept (tag names don't change meaning).
--
-- jsonb_array_elements_text() explodes the JSONB array ["history","american","crime"]
-- into individual text rows so we can treat each tag as its own record.
-- ON CONFLICT (tag_name) DO NOTHING skips tags that already exist in the table.

INSERT INTO warehouse.dim_tag (tag_name)
SELECT DISTINCT t.tag_name
FROM   staging.stg_posts AS s
-- CROSS JOIN LATERAL explodes the tags array — produces one row per tag per post
CROSS  JOIN LATERAL jsonb_array_elements_text(s.raw_payload -> 'tags') AS t(tag_name)
WHERE  t.tag_name IS NOT NULL
ON CONFLICT (tag_name) DO NOTHING;
