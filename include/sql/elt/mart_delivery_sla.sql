CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    store_id,
    AVG(delivery_minutes) AS avg_delivery_minutes,
    ROUND(100.0 * SUM(CASE WHEN delivery_minutes <= 15 AND order_status = 'DELIVERED' THEN 1 ELSE 0 END) / NULLIF(COUNT(CASE WHEN order_status = 'DELIVERED' THEN 1 END), 0), 2) AS pct_within_15_min
FROM stg_orders
WHERE order_status = 'DELIVERED' AND dq_status = 'PASS'
GROUP BY store_id;
