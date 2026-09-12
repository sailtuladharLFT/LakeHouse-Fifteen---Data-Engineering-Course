-- Step 6: Load today's product snapshot into the fact table.
-- Joins staging → dim_product (current row only) → dim_category → dim_date.
-- discountPercentage uses the camelCase key exactly as it appears in the API JSON.

INSERT INTO warehouse.fact_product_snapshot
    (product_sk, category_key, date_key, list_price, discount_pct, stock)
SELECT
    dp.product_sk,
    dc.category_key,
    dd.date_key,
    (s.raw_payload->>'price')::NUMERIC(10,2)             AS list_price,
    (s.raw_payload->>'discountPercentage')::NUMERIC(5,2) AS discount_pct,
    (s.raw_payload->>'stock')::INT                        AS stock
FROM staging.stg_products AS s

-- Join to the current product dimension row
JOIN warehouse.dim_product  AS dp
  ON  dp.product_id  = (s.raw_payload->>'id')::INT
  AND dp.is_current  = TRUE

-- Join to category dimension on the category name
JOIN warehouse.dim_category AS dc
  ON  dc.category_name = (s.raw_payload->>'category')

-- Join to today's date key (YYYYMMDD as INT)
JOIN warehouse.dim_date     AS dd
  ON  dd.date_key = CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT);
