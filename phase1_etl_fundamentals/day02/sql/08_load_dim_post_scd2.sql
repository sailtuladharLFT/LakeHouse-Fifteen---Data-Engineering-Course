-- Step 8: Type 2 SCD logic for dim_post.
-- Runs three statements in order — runner.py executes the whole file at once.
-- Tracked attributes: title and body (post content can be edited by the author).
-- user_id is treated as stable (author doesn't change) so it is not a trigger.
--
-- Statement A: Expire rows where title OR body has changed.
--   Sets is_current = FALSE and valid_to = today on the old version.
--   IS DISTINCT FROM handles NULLs safely (unlike !=, which fails on NULL comparisons).
--
-- Statement B: Insert a new current row for every post that was just expired.
--   The NOT EXISTS guard prevents duplicate inserts on same-day re-runs.
--
-- Statement C: Insert brand-new posts that have never appeared in dim_post.

-- ── A: Expire posts whose title or body changed ──────────────────────────────
UPDATE warehouse.dim_post AS dp
SET    is_current = FALSE,
       valid_to   = CURRENT_DATE
FROM   staging.stg_posts AS s
WHERE  dp.post_id    = (s.raw_payload->>'id')::INT
  AND  dp.is_current = TRUE
  AND  (
           -- title was edited
           (s.raw_payload->>'title') IS DISTINCT FROM dp.title
           OR
           -- body was edited
           (s.raw_payload->>'body')  IS DISTINCT FROM dp.body
       );

-- ── B: Insert new version for every post that was just expired ───────────────
INSERT INTO warehouse.dim_post
    (post_id, title, body, user_id, is_current, valid_from, valid_to)
SELECT
    (s.raw_payload->>'id')::INT,
    s.raw_payload->>'title',
    s.raw_payload->>'body',
    (s.raw_payload->>'userId')::INT,
    TRUE,
    CURRENT_DATE,
    NULL          -- NULL valid_to = still the active record
FROM staging.stg_posts AS s
WHERE EXISTS (
    -- only for posts we just expired (valid_to = today, no longer current)
    SELECT 1
    FROM   warehouse.dim_post AS dp
    WHERE  dp.post_id    = (s.raw_payload->>'id')::INT
      AND  dp.is_current = FALSE
      AND  dp.valid_to   = CURRENT_DATE
)
AND NOT EXISTS (
    -- guard: skip if a current row already exists (re-run safety)
    SELECT 1
    FROM   warehouse.dim_post AS dp
    WHERE  dp.post_id    = (s.raw_payload->>'id')::INT
      AND  dp.is_current = TRUE
);

-- ── C: Insert brand-new posts (no prior dim_post row at all) ─────────────────
INSERT INTO warehouse.dim_post
    (post_id, title, body, user_id, is_current, valid_from, valid_to)
SELECT
    (s.raw_payload->>'id')::INT,
    s.raw_payload->>'title',
    s.raw_payload->>'body',
    (s.raw_payload->>'userId')::INT,
    TRUE,
    CURRENT_DATE,
    NULL
FROM staging.stg_posts AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM   warehouse.dim_post AS dp
    WHERE  dp.post_id = (s.raw_payload->>'id')::INT
);
