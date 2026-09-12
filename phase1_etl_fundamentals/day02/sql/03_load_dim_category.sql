-- Step 4: Type 1 upsert — load distinct categories from raw staging data.
-- Type 1 means we simply insert new categories; no history is kept.
-- ->>'category' extracts the field from JSONB as plain text.
-- ON CONFLICT DO NOTHING skips categories already in the table.

INSERT INTO warehouse.dim_category (category_name)
SELECT DISTINCT raw_payload->>'category'
FROM   staging.stg_products
WHERE  raw_payload->>'category' IS NOT NULL
ON CONFLICT (category_name) DO NOTHING;
