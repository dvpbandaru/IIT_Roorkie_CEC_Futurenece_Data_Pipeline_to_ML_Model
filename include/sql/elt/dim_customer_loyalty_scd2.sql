CREATE TABLE IF NOT EXISTS dim_customer_loyalty_scd2 (
    customer_id VARCHAR,
    loyalty_tier VARCHAR,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);

UPDATE dim_customer_loyalty_scd2 AS history
SET valid_to = current_timestamp, is_current = FALSE
FROM stg_customers AS snapshot
WHERE history.customer_id = snapshot.customer_id
  AND snapshot.dq_status = 'PASS'
  AND history.is_current
  AND history.loyalty_tier IS DISTINCT FROM snapshot.loyalty_tier;

INSERT INTO dim_customer_loyalty_scd2
    (customer_id, loyalty_tier, valid_from, valid_to, is_current)
SELECT snapshot.customer_id, snapshot.loyalty_tier,
       current_timestamp, NULL, TRUE
FROM stg_customers AS snapshot
WHERE snapshot.dq_status = 'PASS'
  AND NOT EXISTS (
      SELECT 1
      FROM dim_customer_loyalty_scd2 AS history
      WHERE history.customer_id = snapshot.customer_id
        AND history.is_current
        AND history.loyalty_tier = snapshot.loyalty_tier
  );
