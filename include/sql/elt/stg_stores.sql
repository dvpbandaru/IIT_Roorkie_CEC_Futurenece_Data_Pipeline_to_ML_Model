CREATE OR REPLACE TABLE stg_stores AS
WITH latest_batch AS (
    SELECT r.*
    FROM raw_stores r
    JOIN (
        SELECT _source_file, MAX(_loaded_at) AS latest_loaded_at
        FROM raw_stores GROUP BY _source_file
    ) latest USING (_source_file)
    WHERE r._loaded_at = latest.latest_loaded_at
), normalized AS (
    SELECT
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        CASE LOWER(TRIM(zone))
            WHEN 'north' THEN 'North'
            WHEN 'south' THEN 'South'
            WHEN 'east' THEN 'East'
            WHEN 'west' THEN 'West'
            ELSE TRIM(zone)
        END AS zone,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(open_date AS DATE) AS open_date,
        TRIM(manager_name) AS manager_name,
        TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
        _source_file,
        _run_id,
        COUNT(*) OVER (PARTITION BY store_id) AS id_count
    FROM latest_batch
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id ORDER BY _loaded_at DESC
    ) = 1
)
SELECT *,
    CASE
        WHEN store_id IS NULL OR store_id = '' THEN 'FAIL'
        WHEN id_count > 1 THEN 'FAIL'
        WHEN zone IS NULL OR zone NOT IN ('North', 'South', 'East', 'West') THEN 'FAIL'
        WHEN open_date IS NULL OR open_date > current_date THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN store_id IS NULL OR store_id = '' THEN 'store_id missing'
        WHEN id_count > 1 THEN 'duplicate store_id'
        WHEN zone IS NULL OR zone NOT IN ('North', 'South', 'East', 'West') THEN 'invalid zone'
        WHEN open_date IS NULL OR open_date > current_date THEN 'open_date invalid or future dated'
        ELSE NULL
    END AS failure_reason
FROM normalized;
