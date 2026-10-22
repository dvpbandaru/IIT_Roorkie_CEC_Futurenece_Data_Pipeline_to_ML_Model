CREATE OR REPLACE TABLE mart_rejected_by_reason AS
SELECT
    _source_file AS source_file,
    failure_reason,
    SUM(CASE WHEN source_order_id_count > 1 THEN source_order_id_count ELSE 1 END) AS rejected_rows
FROM stg_orders
WHERE dq_status = 'FAIL'
GROUP BY _source_file, failure_reason
UNION ALL
SELECT _source_file, failure_reason, COUNT(*)
FROM stg_increment_orders
WHERE dq_status = 'FAIL'
GROUP BY _source_file, failure_reason
UNION ALL
SELECT
    _source_file,
    failure_reason,
    SUM(CASE WHEN id_count > 1 THEN id_count ELSE 1 END)
FROM stg_payments
WHERE dq_status = 'FAIL'
GROUP BY _source_file, failure_reason
UNION ALL
SELECT
    _source_file,
    failure_reason,
    SUM(CASE WHEN id_count > 1 THEN id_count ELSE 1 END)
FROM stg_customers
WHERE dq_status = 'FAIL'
GROUP BY _source_file, failure_reason
UNION ALL
SELECT
    _source_file,
    failure_reason,
    SUM(CASE WHEN id_count > 1 THEN id_count ELSE 1 END)
FROM stg_products
WHERE dq_status = 'FAIL'
GROUP BY _source_file, failure_reason
UNION ALL
SELECT
    _source_file,
    failure_reason,
    SUM(CASE WHEN id_count > 1 THEN id_count ELSE 1 END)
FROM stg_stores
WHERE dq_status = 'FAIL'
GROUP BY _source_file, failure_reason;
