CREATE OR REPLACE TABLE stg_increment_orders AS
WITH increment_batch AS (
    SELECT r.*
    FROM raw_orders r
    JOIN (
        SELECT MAX(_loaded_at) AS latest_loaded_at
        FROM raw_orders
        WHERE _source_file = 'orders_2026-10-01_increment.csv'
    ) latest ON r._loaded_at = latest.latest_loaded_at
    WHERE r._source_file = 'orders_2026-10-01_increment.csv'
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
        _run_id
    FROM increment_batch
), checked AS (
    SELECT *,
        CASE
            WHEN order_id IS NULL OR order_id = '' THEN 'order_id missing'
            WHEN order_ts IS NULL OR order_ts > current_timestamp THEN 'order_ts invalid or future dated'
            WHEN quantity IS NULL OR quantity <= 0 THEN 'quantity must be positive'
            WHEN unit_price IS NULL OR unit_price <= 0 THEN 'unit_price must be positive'
            WHEN discount_pct NOT BETWEEN 0 AND 100 THEN 'discount_pct outside 0 to 100'
            WHEN ABS(ROUND(quantity * unit_price * (1 - discount_pct / 100), 2) - order_amount) > 0.01 THEN 'order_amount formula mismatch'
            WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'order_status invalid'
            WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode invalid'
            WHEN order_status = 'DELIVERED' AND (delivery_minutes IS NULL OR delivery_minutes > 120) THEN 'delivery_minutes invalid'
            WHEN NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id = checked.customer_id AND c.dq_status = 'PASS') THEN 'customer_id not found in valid customers'
            WHEN NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id = checked.product_id AND p.dq_status = 'PASS') THEN 'product_id not found in valid products'
            WHEN NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id = checked.store_id AND s.dq_status = 'PASS') THEN 'store_id not found in valid stores'
            ELSE NULL
        END AS failure_reason
    FROM normalized checked
)
SELECT *,
    CASE WHEN failure_reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    CAST(order_ts AS DATE) AS order_date,
    CASE WHEN order_status = 'DELIVERED' THEN order_amount ELSE 0 END AS net_revenue,
    CASE WHEN order_status = 'DELIVERED' AND delivery_minutes <= 15 THEN TRUE ELSE FALSE END AS within_15_min
FROM checked;

MERGE INTO stg_orders AS target
USING stg_increment_orders AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.dq_status = 'PASS' THEN UPDATE SET
    order_ts = source.order_ts,
    customer_id = source.customer_id,
    store_id = source.store_id,
    product_id = source.product_id,
    quantity = source.quantity,
    unit_price = source.unit_price,
    discount_pct = source.discount_pct,
    order_amount = source.order_amount,
    payment_mode = source.payment_mode,
    order_status = source.order_status,
    channel = source.channel,
    delivery_minutes = source.delivery_minutes,
    _source_file = source._source_file,
    _run_id = source._run_id,
    source_order_id_count = 1,
    failure_reason = source.failure_reason,
    dq_status = source.dq_status,
    order_date = source.order_date,
    net_revenue = source.net_revenue,
    within_15_min = source.within_15_min
WHEN NOT MATCHED AND source.dq_status = 'PASS' THEN INSERT (
    order_id, order_ts, customer_id, store_id, product_id, quantity,
    unit_price, discount_pct, order_amount, payment_mode, order_status,
    channel, delivery_minutes, _source_file, _run_id, source_order_id_count,
    failure_reason, dq_status, order_date, net_revenue, within_15_min
) VALUES (
    source.order_id, source.order_ts, source.customer_id, source.store_id,
    source.product_id, source.quantity, source.unit_price, source.discount_pct,
    source.order_amount, source.payment_mode, source.order_status, source.channel,
    source.delivery_minutes, source._source_file, source._run_id, 1,
    source.failure_reason, source.dq_status, source.order_date, source.net_revenue,
    source.within_15_min
);
