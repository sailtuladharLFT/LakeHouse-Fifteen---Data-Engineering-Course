-- Step 11: Quality check gate for the posts pipeline.
-- Each statement returns 0 rows if the check PASSES.
-- runner.run_quality_checks() raises RuntimeError if any statement returns rows,
-- which marks the quality_check_gate task FAILED and prevents the fact load from running.

-- QC-1: No NULL foreign keys in today's fact rows.
-- post_sk or date_key being NULL means the dim join silently dropped a row.
SELECT
    'null_fk_check'  AS check_name,
    COUNT(*)         AS failing_rows
FROM   warehouse.fact_post_metrics
WHERE  date_key = CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)
  AND  (post_sk IS NULL OR date_key IS NULL)
HAVING COUNT(*) > 0;

-- QC-2: Today's fact row count must be at least as large as the staging row count.
-- A lower count means some posts failed to join to dim_post or dim_date.
SELECT
    'row_count_check'                                  AS check_name,
    COUNT(*)                                           AS fact_rows,
    (SELECT COUNT(*) FROM staging.stg_posts)           AS staging_rows
FROM   warehouse.fact_post_metrics
WHERE  date_key = CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INT)
HAVING COUNT(*) < (SELECT COUNT(*) FROM staging.stg_posts);

-- QC-3: SCD2 integrity — no post_id should have more than one is_current = TRUE row.
-- The partial unique index on dim_post enforces this at write time, but this check
-- catches any edge case where the index was bypassed.
SELECT
    'scd2_integrity_check'  AS check_name,
    post_id,
    COUNT(*)                AS current_count
FROM   warehouse.dim_post
WHERE  is_current = TRUE
GROUP  BY post_id
HAVING COUNT(*) > 1;

-- QC-4: No orphaned post_sk in the fact table (broken FK = fact rows invisible on joins).
-- LEFT JOIN + IS NULL finds fact rows whose dim_post row no longer exists.
SELECT
    'orphan_post_sk_check'  AS check_name,
    f.post_sk
FROM   warehouse.fact_post_metrics AS f
LEFT   JOIN warehouse.dim_post AS dp ON dp.post_sk = f.post_sk
WHERE  dp.post_sk IS NULL
LIMIT  10;
