-- 1. Net revenue by date, zone, and store.
SELECT order_date, zone, store_id, SUM(net_revenue) AS net_revenue
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY order_date, zone, store_id
ORDER BY order_date, zone, store_id;

-- 2. Revenue and gross margin by product category.
SELECT category, SUM(net_revenue) AS revenue, SUM(gross_margin) AS gross_margin
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY category
ORDER BY category;

-- 3. Average delivery time and percentage within 15 minutes by store.
SELECT store_id, AVG(delivery_minutes) AS avg_delivery_minutes,
    100.0 * AVG(CASE WHEN within_15_min THEN 1.0 ELSE 0.0 END) AS pct_within_15_min
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY store_id
ORDER BY store_id;

-- 4. Revenue by payment mode and amount that does not reconcile.
SELECT 'revenue' AS metric, payment_mode AS label, SUM(net_revenue) AS amount
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
UNION ALL
SELECT 'unreconciled' AS metric, 'all mismatches' AS label,
    COALESCE(SUM(ABS(COALESCE(order_amount, 0) - COALESCE(total_paid, 0))), 0) AS amount
FROM etl_payment_reconciliation;

-- 5. Top 10 customers by delivered spend, without contact details.
SELECT f.customer_id, d.full_name, d.loyalty_tier, SUM(f.net_revenue) AS total_spend
FROM etl_fact_orders f
JOIN etl_dim_customer d USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, d.full_name, d.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;

-- 6. Rejected rows by source file and reason.
SELECT source_file, failure_reason, SUM(rejected_rows) AS rejected_rows
FROM etl_rejected_by_reason
GROUP BY source_file, failure_reason
ORDER BY source_file, failure_reason;
