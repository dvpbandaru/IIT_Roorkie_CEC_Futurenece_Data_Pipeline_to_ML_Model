CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.loyalty_tier,
    SUM(o.order_amount) AS total_spend
FROM stg_orders o
LEFT JOIN stg_customers c ON c.customer_id = o.customer_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
