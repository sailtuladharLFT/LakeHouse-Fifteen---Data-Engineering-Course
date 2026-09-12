-- Step 5: Type 2 SCD logic for dim_product.
-- Runs three statements in order — runner.py executes the whole file at once.
--
-- Statement A: Expire rows where price OR rating has changed.
--   Sets is_current = FALSE and valid_to = today on the old version.
--
-- Statement B: Insert a new current row for every product that was just expired.
--   The NOT EXISTS guard prevents duplicates if this runs twice in one day.
--
-- Statement C: Insert brand-new products that have never appeared in dim_product.

-- ── A: Expire changed rows ──────────────────────────────────────────────────
UPDATE warehouse.dim_product AS dp
SET    is_current = FALSE,
       valid_to   = CURRENT_DATE
FROM   staging.stg_products AS s
WHERE  dp.product_id  = (s.raw_payload->>'id')::INT
  AND  dp.is_current  = TRUE
  AND  (
           -- price changed
           (s.raw_payload->>'price')::NUMERIC(10,2)  IS DISTINCT FROM dp.price
           OR
           -- rating changed
           (s.raw_payload->>'rating')::NUMERIC(3,2)  IS DISTINCT FROM dp.rating
       );

-- ── B: Insert new version for every product that was just expired ───────────
INSERT INTO warehouse.dim_product
    (product_id, title, brand, description, price, rating, is_current, valid_from, valid_to)
SELECT
    (s.raw_payload->>'id')::INT,
    s.raw_payload->>'title',
    s.raw_payload->>'brand',
    s.raw_payload->>'description',
    (s.raw_payload->>'price')::NUMERIC(10,2),
    (s.raw_payload->>'rating')::NUMERIC(3,2),
    TRUE,
    CURRENT_DATE,
    NULL          -- NULL valid_to = still the active record
FROM staging.stg_products AS s
WHERE EXISTS (
    -- only for products we just expired (valid_to = today, no longer current)
    SELECT 1
    FROM   warehouse.dim_product AS dp
    WHERE  dp.product_id = (s.raw_payload->>'id')::INT
      AND  dp.is_current = FALSE
      AND  dp.valid_to   = CURRENT_DATE
)
AND NOT EXISTS (
    -- guard: skip if a current row already exists (re-run safety)
    SELECT 1
    FROM   warehouse.dim_product AS dp
    WHERE  dp.product_id = (s.raw_payload->>'id')::INT
      AND  dp.is_current = TRUE
);

-- ── C: Insert brand-new products (no prior dim_product row at all) ──────────
INSERT INTO warehouse.dim_product
    (product_id, title, brand, description, price, rating, is_current, valid_from, valid_to)
SELECT
    (s.raw_payload->>'id')::INT,
    s.raw_payload->>'title',
    s.raw_payload->>'brand',
    s.raw_payload->>'description',
    (s.raw_payload->>'price')::NUMERIC(10,2),
    (s.raw_payload->>'rating')::NUMERIC(3,2),
    TRUE,
    CURRENT_DATE,
    NULL
FROM staging.stg_products AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM   warehouse.dim_product AS dp
    WHERE  dp.product_id = (s.raw_payload->>'id')::INT
);
