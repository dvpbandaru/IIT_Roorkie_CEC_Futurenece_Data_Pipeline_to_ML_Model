CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    s.zone,
    CAST(o.order_ts AS DATE) AS order_date,
    s.store_id,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS net_revenue,
    AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes END) AS avg_delivery_minutes,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
    COUNT(*) AS total_orders
FROM stg_orders o
LEFT JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.dq_status = 'PASS'
GROUP BY s.zone, CAST(o.order_ts AS DATE), s.store_id;
