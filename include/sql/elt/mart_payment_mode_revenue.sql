CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT
    payment_mode,
    SUM(order_amount) AS revenue,
    COUNT(*) AS delivered_orders
FROM stg_orders
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY payment_mode;
