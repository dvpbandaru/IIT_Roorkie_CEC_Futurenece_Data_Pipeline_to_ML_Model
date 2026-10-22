DELETE FROM dq_results
WHERE dag_id = 'nammamart_elt_prasad'
  AND run_id = (
      SELECT _run_id FROM raw_orders ORDER BY _loaded_at DESC LIMIT 1
  );

INSERT INTO dq_results
WITH run_info AS (
    SELECT _run_id AS run_id
    FROM raw_orders
    ORDER BY _loaded_at DESC
    LIMIT 1
), checks AS (
    SELECT
        (SELECT run_id FROM run_info) AS run_id,
        'nammamart_elt_prasad' AS dag_id,
        'required_fields_present' AS check_name,
        'Completeness' AS dimension,
        'Critical' AS severity,
        (SELECT COUNT(*) FROM stg_orders) + (SELECT COUNT(*) FROM stg_increment_orders) AS rows_checked,
        (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%missing%')
          + (SELECT COUNT(*) FROM stg_increment_orders WHERE failure_reason LIKE '%missing%')
          + (SELECT COUNT(*) FROM stg_products WHERE failure_reason LIKE '%missing%')
          + (SELECT COUNT(*) FROM stg_stores WHERE failure_reason LIKE '%missing%') AS rows_failed
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'business_keys_unique', 'Uniqueness', 'Critical',
        (SELECT COUNT(*) FROM raw_orders WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_payments WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_customers WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_products WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_stores WHERE _run_id = (SELECT run_id FROM run_info)),
        (SELECT COALESCE(SUM(duplicate_rows), 0) FROM (
            SELECT COUNT(*) AS duplicate_rows
            FROM raw_orders WHERE _run_id = (SELECT run_id FROM run_info)
            GROUP BY _source_file, order_id HAVING COUNT(*) > 1
        ) order_duplicates)
          + (SELECT COALESCE(SUM(duplicate_rows), 0) FROM (
            SELECT COUNT(*) AS duplicate_rows
            FROM raw_payments WHERE _run_id = (SELECT run_id FROM run_info)
            GROUP BY payment_id HAVING COUNT(*) > 1
        ) payment_duplicates)
          + (SELECT COALESCE(SUM(duplicate_rows), 0) FROM (
            SELECT COUNT(*) AS duplicate_rows
            FROM raw_customers WHERE _run_id = (SELECT run_id FROM run_info)
            GROUP BY customer_id HAVING COUNT(*) > 1
        ) customer_duplicates)
          + (SELECT COALESCE(SUM(duplicate_rows), 0) FROM (
            SELECT COUNT(*) AS duplicate_rows
            FROM raw_products WHERE _run_id = (SELECT run_id FROM run_info)
            GROUP BY product_id HAVING COUNT(*) > 1
        ) product_duplicates)
          + (SELECT COALESCE(SUM(duplicate_rows), 0) FROM (
            SELECT COUNT(*) AS duplicate_rows
            FROM raw_stores WHERE _run_id = (SELECT run_id FROM run_info)
            GROUP BY store_id HAVING COUNT(*) > 1
        ) store_duplicates)
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'allowed_values_and_formats', 'Validity', 'Critical',
        (SELECT COUNT(*) FROM stg_orders) + (SELECT COUNT(*) FROM stg_payments),
        (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%invalid%')
          + (SELECT COUNT(*) FROM stg_increment_orders WHERE failure_reason LIKE '%invalid%')
          + (SELECT COUNT(*) FROM stg_payments WHERE failure_reason LIKE '%invalid%')
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'amount_quantity_and_price_checks', 'Accuracy', 'Critical',
        (SELECT COUNT(*) FROM stg_orders) + (SELECT COUNT(*) FROM stg_products),
        (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%quantity%'
            OR failure_reason LIKE '%amount%' OR failure_reason LIKE '%delivery_minutes%')
          + (SELECT COUNT(*) FROM stg_increment_orders WHERE failure_reason LIKE '%quantity%'
            OR failure_reason LIKE '%amount%' OR failure_reason LIKE '%delivery_minutes%')
          + (SELECT COUNT(*) FROM stg_products WHERE failure_reason LIKE '%price%')
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'orders_and_payments_consistent', 'Consistency', 'Critical',
        (SELECT COUNT(*) FROM stg_orders) + (SELECT COUNT(*) FROM stg_payments),
        (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%not found%')
          + (SELECT COUNT(*) FROM stg_increment_orders WHERE failure_reason LIKE '%not found%')
          + (SELECT COUNT(*) FROM stg_payments WHERE failure_reason LIKE '%not found%')
          + (SELECT COUNT(*) FROM mart_payment_reconciliation)
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'timestamps_current_and_parseable', 'Timeliness', 'Warning',
        (SELECT COUNT(*) FROM stg_orders) + (SELECT COUNT(*) FROM stg_customers),
        (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%future%'
            OR failure_reason LIKE '%order_ts%')
          + (SELECT COUNT(*) FROM stg_increment_orders WHERE failure_reason LIKE '%future%'
            OR failure_reason LIKE '%order_ts%')
          + (SELECT COUNT(*) FROM stg_customers WHERE failure_reason LIKE '%future%')
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'customer_contact_format', 'Validity', 'Warning',
        (SELECT COUNT(*) FROM raw_customers
            WHERE _source_file = 'customers.csv'
              AND _loaded_at = (SELECT MAX(_loaded_at) FROM raw_customers WHERE _source_file = 'customers.csv')),
        (SELECT COUNT(*) FROM raw_customers
            WHERE _source_file = 'customers.csv'
              AND _loaded_at = (SELECT MAX(_loaded_at) FROM raw_customers WHERE _source_file = 'customers.csv')
              AND NOT REGEXP_MATCHES(TRIM(email), '^[^@ ]+@[^@ ]+[.][^@ ]+$'))
          + (SELECT COUNT(*) FROM raw_customers
            WHERE _source_file = 'customers.csv'
              AND _loaded_at = (SELECT MAX(_loaded_at) FROM raw_customers WHERE _source_file = 'customers.csv')
              AND LENGTH(REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g')) <> 10)
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'positive_store_capacity', 'Accuracy', 'Warning',
        (SELECT COUNT(*) FROM stg_stores),
        (SELECT COUNT(*) FROM stg_stores
            WHERE capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0)
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'unit_price_distribution_vs_mrp', 'Accuracy', 'Warning',
        (SELECT COUNT(*) FROM stg_orders WHERE _source_file = 'orders.csv')
          + (SELECT COUNT(*) FROM stg_increment_orders),
        (SELECT COUNT(*) FROM stg_orders o
            JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASS'
            WHERE o._source_file = 'orders.csv' AND o.unit_price > p.mrp * 10)
          + (SELECT COUNT(*) FROM stg_increment_orders o
            JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASS'
            WHERE o.unit_price > p.mrp * 10)
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'order_volume_vs_previous_run', 'Volume', 'Warning',
        (SELECT COUNT(*) FROM raw_orders
            WHERE _run_id = (SELECT run_id FROM run_info)
              AND _source_file = 'orders.csv'),
        CASE
            WHEN (
                SELECT COUNT(*) FROM raw_orders
                WHERE _run_id = (SELECT run_id FROM run_info)
                  AND _source_file = 'orders.csv'
            ) < 0.1 * (
                SELECT rows_checked FROM dq_results
                WHERE dag_id = 'nammamart_elt_prasad'
                  AND check_name = 'order_volume_vs_previous_run'
                  AND run_id <> (SELECT run_id FROM run_info)
                ORDER BY checked_at DESC
                LIMIT 1
            ) THEN 1 ELSE 0
        END
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'latest_order_not_stale', 'Timeliness', 'Warning',
        (SELECT COUNT(*) FROM stg_orders),
        CASE WHEN (SELECT MAX(order_ts) FROM stg_orders)
            < current_timestamp - INTERVAL '1 day' THEN 1 ELSE 0 END
    UNION ALL
    SELECT (SELECT run_id FROM run_info), 'nammamart_elt_prasad',
        'daily_orders_within_store_capacity', 'Accuracy', 'Warning',
        (SELECT COUNT(*) FROM mart_store_capacity),
        (SELECT COUNT(*) FROM mart_store_capacity WHERE over_capacity)
), inserted_checks AS (
    SELECT run_id, dag_id, check_name, dimension, severity, rows_checked,
        rows_failed,
        CASE
            WHEN rows_failed = 0 THEN 'pass'
            WHEN severity = 'Warning' THEN 'warn'
            ELSE 'fail'
        END AS status,
        current_timestamp AS checked_at
    FROM checks
)
SELECT * FROM inserted_checks;

DELETE FROM pipeline_runs
WHERE dag_id = 'nammamart_elt_prasad'
  AND run_id = (
      SELECT _run_id FROM raw_orders ORDER BY _loaded_at DESC LIMIT 1
  );

INSERT INTO pipeline_runs
(run_id, dag_id, status, rows_extracted, rows_loaded, rows_quarantined,
 start_time, end_time, failed_task, failure_reason)
WITH run_info AS (
    SELECT _run_id AS run_id
    FROM raw_orders
    ORDER BY _loaded_at DESC
    LIMIT 1
), counts AS (
    SELECT
        (SELECT run_id FROM run_info) AS run_id,
        (SELECT COUNT(*) FROM raw_orders WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_payments WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_customers WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_products WHERE _run_id = (SELECT run_id FROM run_info))
          + (SELECT COUNT(*) FROM raw_stores WHERE _run_id = (SELECT run_id FROM run_info)) AS rows_read,
        (SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'PASS')
          + (SELECT COUNT(*) FROM stg_payments WHERE dq_status = 'PASS')
          + (SELECT COUNT(*) FROM stg_customers WHERE dq_status = 'PASS')
          + (SELECT COUNT(*) FROM stg_products WHERE dq_status = 'PASS')
          + (SELECT COUNT(*) FROM stg_stores WHERE dq_status = 'PASS') AS rows_passed,
        (SELECT COALESCE(SUM(rejected_rows), 0) FROM mart_rejected_by_reason) AS rows_quarantined
)
SELECT run_id, 'nammamart_elt_prasad',
    CASE WHEN rows_quarantined > 0 THEN 'success_with_rejections' ELSE 'success' END,
    rows_read, rows_passed, rows_quarantined, current_timestamp, current_timestamp,
    NULL, NULL
FROM counts;
