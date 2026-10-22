CREATE OR REPLACE TABLE stg_customers AS
WITH latest_batch AS (
    SELECT r.*
    FROM raw_customers r
    JOIN (
        SELECT _source_file, MAX(_loaded_at) AS latest_loaded_at
        FROM raw_customers GROUP BY _source_file
    ) latest USING (_source_file)
    WHERE r._loaded_at = latest.latest_loaded_at
), normalized AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        'Customer ' || TRIM(customer_id) AS full_name,
        CASE
            WHEN REGEXP_MATCHES(TRIM(email), '^[^@ ]+@[^@ ]+[.][^@ ]+$')
                THEN REGEXP_REPLACE(TRIM(email), '^(.{1,3})[^@]*@', '\1***@')
            ELSE 'masked@invalid.local'
        END AS email,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g')) = 10
                THEN SUBSTR(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 1, 2)
                    || '******' ||
                    SUBSTR(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g'), 9, 2)
            ELSE '**********'
        END AS phone,
        TRIM(city) AS city,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date,
        CASE LOWER(TRIM(loyalty_tier))
            WHEN 'bronze' THEN 'Bronze'
            WHEN 'silver' THEN 'Silver'
            WHEN 'gold' THEN 'Gold'
            WHEN 'platinum' THEN 'Platinum'
            ELSE TRIM(loyalty_tier)
        END AS loyalty_tier,
        UPPER(TRIM(is_active)) AS is_active,
        _source_file,
        _run_id,
        COUNT(*) OVER (PARTITION BY customer_id) AS id_count
    FROM latest_batch
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY _loaded_at DESC
    ) = 1
)
SELECT *,
    CASE
        WHEN customer_id IS NULL OR customer_id = '' THEN 'FAIL'
        WHEN id_count > 1 THEN 'FAIL'
        WHEN full_name IS NULL OR full_name = '' THEN 'FAIL'
        WHEN signup_date IS NULL OR signup_date > current_date THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN customer_id IS NULL OR customer_id = '' THEN 'customer_id missing'
        WHEN id_count > 1 THEN 'duplicate customer_id'
        WHEN full_name IS NULL OR full_name = '' THEN 'full_name missing'
        WHEN signup_date IS NULL OR signup_date > current_date THEN 'signup_date invalid or future dated'
        ELSE NULL
    END AS failure_reason
FROM normalized;
