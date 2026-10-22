CREATE OR REPLACE TABLE stg_payments AS
WITH latest_batch AS (
    SELECT r.*
    FROM raw_payments r
    JOIN (
        SELECT _source_file, MAX(_loaded_at) AS latest_loaded_at
        FROM raw_payments GROUP BY _source_file
    ) latest USING (_source_file)
    WHERE r._loaded_at = latest.latest_loaded_at
), normalized AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
        CASE
            WHEN UPPER(TRIM(payment_mode)) = 'UPI' THEN 'UPI'
            WHEN UPPER(TRIM(payment_mode)) = 'CARD' THEN 'Card'
            WHEN UPPER(REPLACE(TRIM(payment_mode), ' ', '')) IN ('CASHONDELIVERY', 'COD') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        TRIM(gateway_ref) AS gateway_ref,
        _source_file,
        _run_id,
        COUNT(*) OVER (PARTITION BY payment_id) AS id_count
    FROM latest_batch
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payment_id ORDER BY _loaded_at DESC
    ) = 1
)
SELECT *,
    CASE
        WHEN payment_id IS NULL OR payment_id = '' THEN 'FAIL'
        WHEN id_count > 1 THEN 'FAIL'
        WHEN payment_status IS NULL OR payment_status NOT IN ('SUCCESS', 'PENDING', 'FAILED') THEN 'FAIL'
        WHEN payment_mode IS NULL OR payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'FAIL'
        WHEN payment_ts IS NULL OR payment_ts > current_timestamp THEN 'FAIL'
        WHEN amount_paid IS NULL OR amount_paid < 0 THEN 'FAIL'
        WHEN NOT EXISTS (
            SELECT 1 FROM stg_orders o
            WHERE o.order_id = normalized.order_id AND o.dq_status = 'PASS'
        ) THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN payment_id IS NULL OR payment_id = '' THEN 'payment_id missing'
        WHEN id_count > 1 THEN 'duplicate payment_id'
        WHEN payment_status IS NULL OR payment_status NOT IN ('SUCCESS', 'PENDING', 'FAILED') THEN 'invalid payment_status'
        WHEN payment_mode IS NULL OR payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'invalid payment_mode'
        WHEN payment_ts IS NULL OR payment_ts > current_timestamp THEN 'payment_ts invalid or future dated'
        WHEN amount_paid IS NULL OR amount_paid < 0 THEN 'amount_paid invalid'
        WHEN NOT EXISTS (
            SELECT 1 FROM stg_orders o
            WHERE o.order_id = normalized.order_id AND o.dq_status = 'PASS'
        ) THEN 'order_id not found in valid orders'
        ELSE NULL
    END AS failure_reason
FROM normalized;
