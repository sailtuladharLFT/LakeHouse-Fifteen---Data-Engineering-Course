-- Step 7: Quality check gate.
-- Each statement returns 0 rows if the check PASSES.
-- runner.run_quality_checks() raises RuntimeError if any statement returns rows.
-- The Airflow quality_check_gate task will then fail, blocking the fact load.

-- QC-1: No NULL foreign keys in today's fact rows
SELECT
    'null_fk_check'  AS check_name,
    COUNT(*)         AS failing_rows
FROM   warehouse.fact_product_snapshot
WHERE  date_key = CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)
  AND  (product_sk IS NULL OR category_key IS NULL OR date_key IS NULL)
HAVING COUNT(*) > 0;

-- QC-2: Today's fact row count must equal the number of products in staging
SELECT
    'row_count_check'                                          AS check_name,
    COUNT(*)                                                   AS fact_rows,
    (SELECT COUNT(*) FROM staging.stg_products)               AS staging_rows
FROM   warehouse.fact_product_snapshot
WHERE  date_key = CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)
HAVING COUNT(*) < (SELECT COUNT(*) FROM staging.stg_products);

-- QC-3: SCD2 integrity — no product_id should have more than one is_current = TRUE row
SELECT
    'scd2_integrity_check'  AS check_name,
    product_id,
    COUNT(*)                AS current_count
FROM   warehouse.dim_product
WHERE  is_current = TRUE
GROUP  BY product_id
HAVING COUNT(*) > 1;

-- QC-4: No orphaned category_key in the fact table (broken FK would mean data loss on joins)
SELECT
    'orphan_category_fk_check'  AS check_name,
    f.category_key
FROM   warehouse.fact_product_snapshot AS f
LEFT   JOIN warehouse.dim_category AS dc ON dc.category_key = f.category_key
WHERE  dc.category_key IS NULL
LIMIT  10;
