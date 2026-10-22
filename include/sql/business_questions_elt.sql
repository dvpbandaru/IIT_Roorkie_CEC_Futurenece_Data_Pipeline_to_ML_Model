-- 1. Net revenue by date, zone, and store.
SELECT order_date, zone, store_id, SUM(net_revenue) AS net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
GROUP BY order_date, zone, store_id
ORDER BY order_date, zone, store_id;

-- 2. Revenue and gross margin by product category.
SELECT category, revenue, gross_margin
FROM mart_category_margin
ORDER BY category;

-- 3. Average delivery time and percentage within 15 minutes by store.
SELECT store_id, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY store_id;

-- 4. Revenue by payment mode and total amount that does not reconcile.
SELECT 'revenue' AS metric, payment_mode AS label, SUM(revenue) AS amount
FROM mart_payment_mode_revenue
GROUP BY payment_mode
UNION ALL
SELECT 'unreconciled' AS metric, 'all mismatches' AS label,
    COALESCE(SUM(ABS(COALESCE(order_amount, 0) - COALESCE(total_paid, 0))), 0) AS amount
FROM mart_payment_reconciliation;

-- 5. Top 10 customers by delivered spend, without contact details.
SELECT customer_id, full_name, loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC;

-- 6. Rejected rows by source file and reason.
SELECT source_file, failure_reason, SUM(rejected_rows) AS rejected_rows
FROM mart_rejected_by_reason
GROUP BY source_file, failure_reason
ORDER BY source_file, failure_reason;
