CREATE OR REPLACE TABLE mart_store_capacity AS
SELECT
    s.store_id,
    s.zone,
    o.order_date,
    COUNT(*) AS daily_orders,
    s.capacity_orders_per_day,
    COUNT(*) > s.capacity_orders_per_day AS over_capacity
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.dq_status = 'PASS'
  AND s.dq_status = 'PASS'
GROUP BY s.store_id, s.zone, o.order_date, s.capacity_orders_per_day;
