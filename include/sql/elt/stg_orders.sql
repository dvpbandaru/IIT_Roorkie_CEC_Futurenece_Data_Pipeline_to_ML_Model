CREATE OR REPLACE TABLE stg_orders AS
WITH latest_file_times AS (
    SELECT _source_file, MAX(_loaded_at) AS latest_loaded_at
    FROM raw_orders
    WHERE _source_file = 'orders.csv'
    GROUP BY _source_file
), latest_file_loads AS (
    SELECT *
    FROM raw_orders
    JOIN latest_file_times USING (_source_file)
    WHERE _loaded_at = latest_loaded_at
), ranked AS (
    SELECT *,
        COUNT(*) OVER (PARTITION BY order_id, _source_file) AS source_order_id_count,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _loaded_at DESC,
                CASE WHEN _source_file LIKE '%increment%' THEN 1 ELSE 0 END DESC
        ) AS order_version
    FROM latest_file_loads
), normalized AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        CASE
            WHEN UPPER(TRIM(payment_mode)) = 'UPI' THEN 'UPI'
            WHEN UPPER(TRIM(payment_mode)) = 'CARD' THEN 'Card'
            WHEN UPPER(REPLACE(TRIM(payment_mode), ' ', '')) IN ('CASHONDELIVERY', 'COD') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        TRIM(channel) AS channel,
        TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        _source_file,
        _run_id,
        source_order_id_count,
        CASE
            WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'order_id missing'
            WHEN source_order_id_count > 1 THEN 'duplicate order_id in current source files'
            WHEN TRY_CAST(order_ts AS TIMESTAMP) IS NULL THEN 'order_ts missing or invalid'
            WHEN TRY_CAST(order_ts AS TIMESTAMP) > current_timestamp THEN 'order_ts is in the future'
            WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'customer_id missing'
            WHEN store_id IS NULL OR TRIM(store_id) = '' THEN 'store_id missing'
            WHEN product_id IS NULL OR TRIM(product_id) = '' THEN 'product_id missing'
            WHEN TRY_CAST(quantity AS INTEGER) IS NULL OR TRY_CAST(quantity AS INTEGER) <= 0 THEN 'quantity must be positive'
            WHEN TRY_CAST(unit_price AS DOUBLE) IS NULL OR TRY_CAST(unit_price AS DOUBLE) <= 0 THEN 'unit_price must be positive'
            WHEN TRY_CAST(discount_pct AS DOUBLE) IS NULL OR TRY_CAST(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100 THEN 'discount_pct outside 0 to 100'
            WHEN TRY_CAST(order_amount AS DOUBLE) IS NULL OR ABS(ROUND(TRY_CAST(quantity AS DOUBLE) * TRY_CAST(unit_price AS DOUBLE) * (1 - TRY_CAST(discount_pct AS DOUBLE) / 100), 2) - TRY_CAST(order_amount AS DOUBLE)) > 0.01 THEN 'order_amount formula mismatch'
            WHEN order_status IS NULL OR UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'order_status invalid'
            WHEN payment_mode IS NULL OR UPPER(REPLACE(TRIM(payment_mode), ' ', '')) NOT IN ('UPI', 'CARD', 'WALLET', 'CASHONDELIVERY', 'COD') THEN 'payment_mode invalid'
            WHEN UPPER(TRIM(order_status)) = 'DELIVERED' AND TRY_CAST(delivery_minutes AS DOUBLE) IS NULL THEN 'delivered order missing delivery_minutes'
            WHEN UPPER(TRIM(order_status)) = 'DELIVERED' AND TRY_CAST(delivery_minutes AS DOUBLE) > 120 THEN 'delivery_minutes exceeds 120'
            WHEN NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id = TRIM(ranked.customer_id) AND c.dq_status = 'PASS') THEN 'customer_id not found in valid customers'
            WHEN NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id = TRIM(ranked.product_id) AND p.dq_status = 'PASS') THEN 'product_id not found in valid products'
            WHEN NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id = TRIM(ranked.store_id) AND s.dq_status = 'PASS') THEN 'store_id not found in valid stores'
            ELSE NULL
        END AS failure_reason
    FROM ranked
    WHERE order_version = 1
)
SELECT
    *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    TRY_CAST(order_ts AS DATE) AS order_date,
    CASE WHEN order_status = 'DELIVERED' THEN order_amount ELSE 0 END AS net_revenue,
    CASE WHEN order_status = 'DELIVERED' AND delivery_minutes <= 15 THEN TRUE ELSE FALSE END AS within_15_min
FROM normalized;
