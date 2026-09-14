-- Step 10: Load today's post metrics snapshot into the fact table.
-- Joins staging → dim_post (current row only) → dim_date (today's date key).
-- user_id is carried over from staging for convenient author-level filtering.
-- Tag analysis is done separately via bridge_post_tag → dim_tag (not joined here).
--
-- reactions is a nested JSONB object: {"likes": 192, "dislikes": 25}
-- Use -> to navigate into the object, then ->> to extract as text before casting.

INSERT INTO warehouse.fact_post_metrics
    (post_sk, date_key, user_id, likes, dislikes, views)
SELECT
    dp.post_sk,
    dd.date_key,
    (s.raw_payload->>'userId')::INT                          AS user_id,
    (s.raw_payload->'reactions'->>'likes')::INT              AS likes,
    (s.raw_payload->'reactions'->>'dislikes')::INT           AS dislikes,
    (s.raw_payload->>'views')::INT                           AS views
FROM staging.stg_posts AS s

-- Join to the current post dimension row only
JOIN warehouse.dim_post AS dp
  ON  dp.post_id    = (s.raw_payload->>'id')::INT
  AND dp.is_current = TRUE

-- Join to today's date key (YYYYMMDD as INT)
JOIN warehouse.dim_date AS dd
  ON  dd.date_key = CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT);
