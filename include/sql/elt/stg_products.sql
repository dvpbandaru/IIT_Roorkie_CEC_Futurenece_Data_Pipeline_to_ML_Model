CREATE OR REPLACE TABLE stg_products AS
WITH latest_batch AS (
    SELECT r.*
    FROM raw_products r
    JOIN (
        SELECT _source_file, MAX(_loaded_at) AS latest_loaded_at
        FROM raw_products GROUP BY _source_file
    ) latest USING (_source_file)
    WHERE r._loaded_at = latest.latest_loaded_at
), normalized AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(brand) AS brand,
        TRY_CAST(mrp AS DOUBLE) AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        TRIM(unit) AS unit,
        LOWER(TRIM(is_perishable)) IN ('true', 't', '1', 'y') AS is_perishable,
        TRY_CAST(launch_date AS DATE) AS launch_date,
        _source_file,
        _run_id,
        COUNT(*) OVER (PARTITION BY product_id) AS id_count
    FROM latest_batch
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id ORDER BY _loaded_at DESC
    ) = 1
)
SELECT *,
    CASE
        WHEN product_id IS NULL OR product_id = '' THEN 'FAIL'
        WHEN id_count > 1 THEN 'FAIL'
        WHEN category IS NULL OR category = '' THEN 'FAIL'
        WHEN mrp IS NULL OR mrp <= 0 OR cost_price IS NULL OR cost_price > mrp THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN product_id IS NULL OR product_id = '' THEN 'product_id missing'
        WHEN id_count > 1 THEN 'duplicate product_id'
        WHEN category IS NULL OR category = '' THEN 'category missing'
        WHEN mrp IS NULL OR mrp <= 0 OR cost_price IS NULL OR cost_price > mrp THEN 'invalid mrp or cost_price'
        ELSE NULL
    END AS failure_reason
FROM normalized;
