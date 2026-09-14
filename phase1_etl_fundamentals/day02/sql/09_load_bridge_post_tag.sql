-- Step 9: Populate bridge_post_tag — maps each dim_post surrogate key to its dim_tag keys.
-- Must run AFTER both 07_load_dim_tag and 08_load_dim_post_scd2 so all FKs exist.
--
-- Why a bridge?
--   A post can have multiple tags (many-to-many). Putting a single tag_key FK on the
--   fact table would force one row per tag per post, inflating every aggregate (SUM of
--   views, likes) by the number of tags. The bridge keeps fact rows clean (one per post
--   per day) while still allowing tag-level analysis via a JOIN.
--
-- How it works:
--   CROSS JOIN LATERAL jsonb_array_elements_text() explodes the tags array into rows,
--   then we look up tag_key and the current post_sk for each tag.
--   ON CONFLICT (post_sk, tag_key) DO NOTHING makes re-runs safe.

INSERT INTO warehouse.bridge_post_tag (post_sk, tag_key)
SELECT
    dp.post_sk,
    dt.tag_key
FROM staging.stg_posts AS s

-- Explode the JSONB tags array: ["history","american","crime"] → three rows
CROSS JOIN LATERAL jsonb_array_elements_text(s.raw_payload -> 'tags') AS t(tag_name)

-- Look up the surrogate key for each tag name
JOIN warehouse.dim_tag  AS dt ON dt.tag_name = t.tag_name

-- Only join the current version of the post — SCD2 may have old rows too
JOIN warehouse.dim_post AS dp
  ON  dp.post_id    = (s.raw_payload->>'id')::INT
  AND dp.is_current = TRUE

-- UNIQUE(post_sk, tag_key) constraint on the table makes this idempotent
ON CONFLICT (post_sk, tag_key) DO NOTHING;
