-- Step 3: Populate dim_date for 2024-01-01 through 2030-12-31
-- generate_series produces one timestamp per day in the range.
-- ON CONFLICT DO NOTHING makes this safe to re-run (idempotent).

INSERT INTO warehouse.dim_date (date_key, full_date, year, month, day, quarter)
SELECT
    -- date_key as YYYYMMDD integer — efficient for joins in the fact table
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INT)   AS date_key,
    d::DATE                                AS full_date,
    EXTRACT(YEAR    FROM d)::INT           AS year,
    EXTRACT(MONTH   FROM d)::INT           AS month,
    EXTRACT(DAY     FROM d)::INT           AS day,
    EXTRACT(QUARTER FROM d)::INT           AS quarter
FROM generate_series(
    '2024-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (date_key) DO NOTHING;
