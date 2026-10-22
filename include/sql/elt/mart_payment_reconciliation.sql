CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH clean_orders AS (
    SELECT order_id, order_amount, order_status
    FROM stg_orders
    WHERE dq_status = 'PASS'
), clean_payments AS (
    SELECT order_id, SUM(amount_paid) AS total_paid
    FROM stg_payments
    WHERE payment_status = 'SUCCESS'
      AND (
          dq_status = 'PASS'
          OR failure_reason = 'order_id not found in valid orders'
      )
    GROUP BY order_id
), order_mismatches AS (
    SELECT
        o.order_id,
        o.order_amount,
        p.total_paid,
        CASE
            WHEN o.order_status IN ('CANCELLED', 'RETURNED')
              AND p.total_paid IS NOT NULL THEN 'payment exists for non-delivered order'
            WHEN p.total_paid IS NULL THEN 'successful payment missing'
            ELSE 'amount mismatch'
        END AS reason
    FROM clean_orders o
    LEFT JOIN clean_payments p ON p.order_id = o.order_id
    WHERE (
        o.order_status = 'DELIVERED'
        AND (p.total_paid IS NULL OR ABS(o.order_amount - p.total_paid) > 0.01)
    ) OR (
        o.order_status IN ('CANCELLED', 'RETURNED')
        AND p.total_paid IS NOT NULL
    )
), orphan_payments AS (
    SELECT
        p.order_id,
        NULL::DOUBLE AS order_amount,
        SUM(p.amount_paid) AS total_paid,
        'payment has no matching order' AS reason
    FROM stg_payments p
    LEFT JOIN clean_orders o ON o.order_id = p.order_id
    WHERE p.payment_status = 'SUCCESS'
      AND (
          p.dq_status = 'PASS'
          OR p.failure_reason = 'order_id not found in valid orders'
      )
      AND o.order_id IS NULL
    GROUP BY p.order_id
)
SELECT * FROM order_mismatches
UNION ALL
SELECT * FROM orphan_payments;
