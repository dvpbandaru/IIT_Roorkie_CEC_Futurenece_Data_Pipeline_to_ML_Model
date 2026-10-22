CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS revenue,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN (o.order_amount - (o.quantity * p.cost_price)) ELSE 0 END) AS gross_margin
FROM stg_orders o
LEFT JOIN stg_products p ON p.product_id = o.product_id
WHERE o.dq_status = 'PASS'
GROUP BY p.category;
