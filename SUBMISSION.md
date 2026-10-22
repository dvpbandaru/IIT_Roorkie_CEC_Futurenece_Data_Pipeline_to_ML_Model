# NammaMart ETL and ELT assignment

I built both pipelines and checked their transformations locally. The Airflow DAGs parse, but I could not complete a green Airflow run in this environment: the DAG test was killed with exit code 137, ELT's latest scheduled run failed, and ETL still has queued/scheduled tasks. I have not made screenshots or claimed results from Airflow experiments I did not run.

## Part 0: Data issues log

I profiled all six supplied files. Duplicate counts below describe extra rows beyond one row per key unless stated otherwise.

| File | Key / column | Finding | Quality dimension | Rule and severity |
| --- | --- | --- | --- | --- |
| `orders.csv` | `order_id` | 12 extra duplicate rows | Uniqueness | Reject duplicate keys; Critical |
| `orders.csv` | `customer_id`, `product_id`, `store_id` | 18 missing customer IDs; 9 references absent from the product file and 5 absent from the store file. More rows refer to invalid master records. | Completeness, consistency | Require IDs and valid master references; Critical |
| `orders.csv` | `order_ts` | 2 missing timestamps; future timestamps also occur | Completeness, timeliness | Require a parseable, non-future timestamp; Critical |
| `orders.csv` | `order_status`, `payment_mode` | 3 unknown statuses and 3 unrecognized payment-mode variants | Validity | Normalize known variants and reject unknown values; Critical |
| `orders.csv` | `quantity`, `order_amount`, `discount_pct` | 10 non-positive quantities; 18 amount/formula issues | Accuracy | Require positive quantity, a 0–100 discount, and the stated amount formula; Critical |
| `orders.csv` | `delivery_minutes` | 8 delivered orders have no time; 6 exceed 120 minutes | Completeness, accuracy | Require a time for delivered orders; quarantine values above 120; Critical |
| `payments.csv` | `payment_id`, `order_id` | 4 extra duplicate payment IDs; 4 extra duplicate order references; 8 rows refer to orders absent from the order file | Uniqueness, consistency | Require unique payment IDs and reconcile each successful payment; Critical |
| `payments.csv` | `gateway_ref`, `payment_mode`, `payment_status`, `amount_paid`, `payment_ts` | 301 missing gateway references; malformed modes/statuses, negative amounts, and future timestamps also occur | Completeness, validity, accuracy, timeliness | Require valid payment fields; missing gateway references on successful payments are warnings |
| `customers.csv` | `customer_id` | 5 extra duplicate rows | Uniqueness | Keep one customer per key; Critical |
| `customers.csv` | `email`, `city`, `signup_date`, `loyalty_tier` | 12 missing/invalid emails; 6 blank cities; future signup dates; mixed tier casing | Validity, completeness, consistency, timeliness | Mask contact details, normalize tiers, and flag incomplete or future values; Warning except future dates |
| `products.csv` | `product_id`, `category`, `cost_price`, `mrp` | 2 extra duplicate rows; 3 missing categories; 5 cost/MRP problems | Uniqueness, completeness, accuracy | Require one product per ID, a category, positive MRP, and cost not above MRP; Critical |
| `stores.csv` | `zone`, `capacity_orders_per_day` | One missing zone and one non-positive capacity | Completeness, accuracy | Require a supported zone and positive capacity; Critical for zone, Warning for capacity |
| `orders_2026-10-01_increment.csv` | `order_id`, references, timestamp | 160 rows: 10 existing IDs and 150 new IDs; no duplicate IDs within the file. 24 rows fail validation. | Consistency, timeliness | Apply only valid corrections/inserts; retain every raw row for audit |

The input files contain 5,372 rows in total. The ETL validator rejects all members of duplicate groups; the ELT staging tables keep one representative row and record the duplicate group count. This is why some per-reason duplicate counts differ by pipeline.

Questions I would confirm with the client:

1. Can a negative quantity represent a return, or is it always an error?
2. Should delivery times above 120 minutes be rejected or loaded with a warning?
3. Should invalid email or phone details block an order from reporting?

## Pipeline implementation

- ETL: [dags/nammamart_etl_prasad.py](./dags/nammamart_etl_prasad.py) extracts the five base files in parallel, validates rows in Python, writes reasoned quarantine CSVs, masks customer details, and loads an idempotent star schema. It also loads the increment and builds payment reconciliation and DQ/audit tables.
- ELT: [dags/nammamart_elt_prasad.py](./dags/nammamart_elt_prasad.py) appends raw text records with source file and run ID, then uses SQL in [include/sql/elt/](./include/sql/elt/) for staging, valid-only increment `MERGE`, marts, DQ, and run audit. Invalid increment updates do not replace a valid current order.
- Shared validation and masking are in [include/nammamart_pipeline_utils.py](./include/nammamart_pipeline_utils.py). Business-question SQL is in [include/sql/business_questions_etl.sql](./include/sql/business_questions_etl.sql) and [include/sql/business_questions_elt.sql](./include/sql/business_questions_elt.sql).

## Part C: Schedule and timezone

Both DAGs use cron `0 9 * * 1-6`, a timezone-aware `Asia/Kolkata` start date, `catchup=False`, 2 retries, a 5-minute retry delay, and a 30-minute execution timeout. 9:00 AM IST is 03:30 UTC in the Airflow UI. If the start date were UTC, the cron would run at 9:00 UTC (2:30 PM IST). If the laptop or Docker is off, the local scheduler cannot run; with catchup disabled, a missed run is not automatically backfilled, so check the run history and trigger it if needed.

## Part D: Data quality checks

Both pipelines record checks in `dq_results` with the run ID, DAG, dimension, severity, checked/failed counts, status, and timestamp.

| Dimension | Example checks | Severity |
| --- | --- | --- |
| Completeness | Required IDs and timestamps; delivered orders need delivery time; products need category | Critical |
| Uniqueness | Order, payment, customer, product, and store keys | Critical |
| Validity | Status and payment-mode lists; parseable timestamps; contact formats | Critical; contact-format warnings |
| Accuracy | Positive quantity/prices; amount formula; delivery-time limit; store capacity; price outlier vs MRP | Critical; capacity and outlier warnings |
| Consistency | Order references to valid masters; payment reconciliation | Critical |
| Timeliness | No future timestamps; latest-order freshness warning | Critical for future dates; Warning for stale data |

## Business questions and observed results

The SQL files above contain all six queries for both pipelines. I ran them against the local DuckDB warehouse after the direct task/SQL checks. Revenue and reconciliation totals match between ETL and ELT.

### 1. Daily net revenue by date, zone, and store

Both queries return 429 delivered date/zone/store groups. September revenue is **950,825.60** in both pipelines. The valid October increment adds **69,785.05**, for **1,020,610.65** across the current tables.

### 2. Revenue and gross margin by category

| Category | Revenue | Gross margin |
| --- | ---: | ---: |
| Dairy & Bakery | 162,104.55 | 43,233.34 |
| Fruits & Vegetables | 84,311.05 | 19,800.10 |
| Household | 180,367.10 | 45,936.04 |
| Personal Care | 192,388.35 | 43,164.60 |
| Snacks & Beverages | 164,253.70 | 38,671.31 |
| Staples | 237,185.90 | 60,353.92 |
| **Total** | **1,020,610.65** | **251,159.31** |

### 3. Delivery time and 15-minute service level by store

Across 1,888 delivered orders, the average delivery time is **21.379 minutes** and **28.02%** were within 15 minutes. The per-store result is the same in both pipelines:

| Store | Average minutes | Within 15 minutes |
| --- | ---: | ---: |
| S01 | 20.69 | 30.71% |
| S02 | 20.83 | 31.82% |
| S03 | 20.89 | 27.07% |
| S04 | 21.74 | 25.60% |
| S05 | 21.37 | 28.46% |
| S06 | 22.26 | 22.70% |
| S08 | 20.54 | 35.76% |
| S09 | 21.15 | 28.23% |
| S10 | 21.57 | 28.15% |
| S11 | 20.94 | 27.86% |
| S12 | 21.92 | 25.00% |
| S13 | 21.64 | 27.69% |
| S14 | 21.67 | 30.63% |
| S15 | 22.23 | 20.86% |

### 4. Revenue by payment mode and amount not reconciled

| Payment mode / measure | Amount |
| --- | ---: |
| Wallet revenue | 121,468.60 |
| Card revenue | 210,198.40 |
| Cash on Delivery revenue | 150,030.10 |
| UPI revenue | 538,913.55 |
| Absolute unreconciled amount across the mismatch table | 368,118.80 |

The reconciliation table has 610 rows in each pipeline: 395 payments without a matching valid order, 196 delivered orders without a successful payment, 11 amount mismatches, and 8 payments for non-delivered orders.

### 5. Top customers by delivered spend

Names are replaced with `Customer <ID>`; no email or phone is returned.

| Customer ID | Loyalty tier | Spend |
| --- | --- | ---: |
| C00136 | Bronze | 8,149.10 |
| C00115 | Bronze | 7,337.70 |
| C00310 | Bronze | 7,258.95 |
| C00236 | Platinum | 7,151.60 |
| C00072 | Silver | 6,393.95 |
| C00338 | Silver | 6,354.55 |
| C00283 | Bronze | 6,241.40 |
| C00129 | Bronze | 6,215.10 |
| C00135 | Bronze | 6,021.20 |
| C00079 | Bronze | 6,020.00 |

### 6. Rejected rows by file and reason

The queries return these reason counts from the direct local run. Duplicate-key counts include every row in a duplicate group.

| File | ETL reasons (rows) | ETL total | ELT reasons (rows) | ELT total |
| --- | --- | ---: | --- | ---: |
| `orders.csv` | Missing customer 18; customer ref 43; product ref 156; store ref 162; duplicate ID 24; formula 15; formula/discount 3; bad status 3; future timestamp 5; missing/invalid timestamp 6; bad mode 2; non-positive quantity 10; missing delivery time 8; delivery over 120 min 6 | 461 | Missing customer 18; customer ref 43; product ref 156; store ref 162; duplicate ID 24; formula 15; discount 3; bad status 3; future timestamp 5; missing/invalid timestamp 6; bad mode 2; non-positive quantity 10; missing delivery time 8; delivery over 120 min 6 | 461 |
| `orders_2026-10-01_increment.csv` | Customer ref 2; product ref 17; store ref 5 | 24 | Customer ref 2; product ref 17; store ref 5 | 24 |
| `payments.csv` | Negative/missing amount 6; duplicate payment ID 8; bad mode 2; bad status 68; future/invalid timestamp 5 | 89 | Negative/missing amount 6; duplicate payment ID 8; bad mode 2; bad status 68; order not in valid staging 401; future/invalid timestamp 5 | 490 |
| `customers.csv` | Duplicate ID 10; future/invalid signup 3 | 13 | Duplicate ID 10; future/invalid signup 3 | 13 |
| `products.csv` | Missing category 3; cost above MRP 4; cost above MRP with invalid MRP 1; duplicate ID 4 | 12 | Missing category 3; invalid MRP/cost 5; duplicate ID 4 | 12 |
| `stores.csv` | Missing/unsupported zone 1 | 1 | Missing/unsupported zone 1 | 1 |

The ELT payment total includes 401 rows whose order does not exist in valid staging; ETL keeps those otherwise-valid payment rows and reports unmatched payments in the reconciliation table instead.

## Part E: Data governance

### 1. Ownership

Operations owns orders and delivery rules; Finance owns payments and reconciliation; CRM owns customers; Merchandising owns products; Store Operations owns stores. Data Engineering maintains the warehouse. Operations and Finance should approve a change such as the 120-minute delivery threshold.

### 2. Data dictionary

| Table / columns | Type and meaning | Source and allowed values | PII |
| --- | --- | --- | --- |
| `etl_fact_orders.order_id`, `customer_id`, `store_id`, `product_id` | Text keys; `customer_id` links to the customer dimension | Orders and master files | IDs are linkable; treat as pseudonymous |
| `etl_fact_orders.order_ts`, `order_date` | Timestamp and date of order | Orders; timestamp must parse and not be future dated | No direct PII |
| `etl_fact_orders.quantity`, `order_amount`, `net_revenue`, `gross_margin`, `delivery_minutes` | Integer/number measures | Orders and product cost; amounts follow the order formula | No direct PII |
| `etl_fact_orders.order_status`, `payment_mode`, `within_15_min` | Text categories and Boolean service flag | Status: `DELIVERED`, `CANCELLED`, `RETURNED`; mode: `UPI`, `Card`, `Cash on Delivery`, `Wallet` | No direct PII |
| `etl_dim_customer.customer_id`, `full_name`, `email`, `phone`, `city`, `loyalty_tier` | Text identity, masked contact/location, and tier | CRM; tier: Bronze, Silver, Gold, Platinum | Customer ID and location are linkable; name is synthetic; contacts are masked |
| `etl_dim_product.product_id`, `product_name`, `category`, `cost_price` | Text identifiers/labels and numeric cost | Product catalogue; category must be present | No direct PII |
| `etl_dim_store.store_id`, `store_name`, `zone`, `capacity_orders_per_day` | Text identifiers/labels and integer capacity | Store master; supported zone and positive capacity | No manager name in the reporting dimension |

### 3. Lineage

For `mart_daily_store_kpis`, the 1 September, East, S08 revenue of **2,984.70** comes from four delivered orders: `ORD000008`, `ORD000051`, `ORD000056`, and `ORD000079` in `orders.csv`. ELT raw rows retain `_source_file` and `_run_id`, and the staging rows retain those keys; the mart still needs an order-ID join to reach them. ETL can be matched to the source by `order_id`, but its fact table does not store source metadata, so ELT is easier to trace.

### 4. Personal data and privacy

Customer names, email, phone, city, area, and pincode can identify or locate a person; customer IDs are linkable. Store `manager_name` may also be personal data. ETL and ELT reporting/staging use a synthetic customer label plus masked email and phone; raw input remains unmasked for audit and must be restricted. Masking preserves useful grouping while reducing exposure. NammaMart should follow the applicable requirements of India's Digital Personal Data Protection Act, 2023, including purpose limitation, safeguards, and approved retention/access practices.

### 5. Access control

Only authorized data engineers, privacy staff, and approved auditors should read raw customer data or quarantine files. Analytics engineers may read validated, masked staging; business users should use approved marts. No analyst should have routine access to unmasked raw phone or email.

### 6. Retention

Legal and Finance should set the final period. A proposed starting point is 12 months for raw and quarantine data, followed by approved deletion or archival. Keep masked marts only for a documented business need and any required financial retention period.

### 7. Secrets

Use Airflow Connections or Variables for credentials such as `SLACK_WEBHOOK_URL`. No webhook was configured or tested here. Credentials must not be placed in DAG code or Git because repository readers and logs could expose them.

### 8. Audit example

The rejected increment row `ORD002431` is preserved in raw storage and the source file as received:

```text
ORD002431,2026-10-01 06:04:00,C00100,S05,P0074,2,350.0,0,700.0,Card,DELIVERED,App,8
```

Both pipelines reject it because product `P0074` is not in the valid product master. It has no row in `etl_fact_orders` and no passing row in `stg_orders`, so it cannot contribute to delivered revenue. The source row and failure reason remain available for review.

### 9. Change history

The ELT raw layer keeps both original and increment rows. A valid correction to `RETURNED` updates the current row, so it no longer contributes to current delivered revenue; ETL applies the same valid-only update. After the accepted increment, September revenue is **950,825.60** in both pipelines. Finance should retain an immutable September-close snapshot if it also needs the value as originally reported; the current-state view is the corrected version.

## Part F: Observability

The run audit uses `run_id`, `dag_id`, `start_time`, `end_time`, `rows_extracted`, `rows_loaded`, `rows_quarantined`, and `status`. Local direct execution recorded:

| Pipeline | Status | Extracted | Loaded / passed | Not loaded / quarantined |
| --- | --- | ---: | ---: | ---: |
| ETL | `success_with_rejections` | 5,372 | 4,772 | 600 |
| ELT | `success_with_rejections` | 5,372 | 4,363 | 1,001 |

ELT's reason-based quarantine total includes duplicate source rows after weighting duplicate groups by their raw row count. The 8 valid old order versions replaced by accepted corrections are superseded, not quarantined. These are local direct-run records, not completed Airflow DAG runs.

| # | Experiment | Expected | Observed | Where checked | Next step |
| --- | --- | --- | --- | --- | --- |
| 1 | Happy path | Both pipelines process inputs and explain rejects | Direct ETL task callables and ELT SQL completed; ELT's latest scheduled Airflow run failed and ETL tasks remain queued/scheduled | Local DuckDB, quarantine outputs, `dq_results`, `pipeline_runs`, Airflow UI | Resolve Airflow execution and capture successful Graph views |
| 2 | Re-run | ETL facts stay stable; ELT raw grows by each load while marts rebuild | Direct transformations rebuilt the same current revenue; Airflow raw reload behavior not verified | Local fact/mart totals | Compare raw row counts across two Airflow runs |
| 3 | Increment | Valid updates replace current versions; new orders insert once | 136 accepted: 8 updates and 128 inserts; 24 invalid rows not merged | Direct ETL run and ELT `stg_increment_orders` | Verify the same counts in Airflow task logs |
| 4 | Missing file | Retries then a visible failure | Not run | No source was renamed | Test with a disposable source copy |
| 5 | New bad rows | Each bad row gets a reason and DQ result | Not run | No modified-file test | Add the five specified rows to a copy and inspect quarantine |
| 6 | Schema change | Missing column fails clearly | Not run | No renamed-column test | Rename `order_amount` in a copy and verify the task error |
| 7 | Volume anomaly | A 50-row feed triggers a warning against the prior run | The 50-row copy warned against the prior 2,412-row run | Direct ETL validator probe on a disposable copy; 50 checked, 1 warning | Confirm the result in Airflow and record the mart impact |
| 8 | Distribution drift | A 100x price change is flagged | The 100x price copy triggered the MRP warning for 1 row | Direct ETL validator probe on a disposable copy; 2,412 checked, 1 warning | Confirm the result in Airflow and record the mart impact |
| 9 | Freshness | A stale feed produces a warning | The timestamp check is implemented; a 30-day-old feed experiment was not run | ELT DQ scorecard on supplied data | Run the specified stale-data copy in Airflow |
| 10 | Recovery | Clear reruns the failed task and downstream tasks | Not run | No failure/recovery UI test | Force a disposable failure and record task states |

The DAGs parse without import errors. I ran experiments 7 and 8 through the local ETL validator on disposable copies, but have not verified either through Airflow or ELT. Freshness has an explicit DQ check, and the MRP comparison provides a basic distribution warning. Exact mart-to-source lineage is still incomplete, and schema failures rely on expected-column access. The Airflow experiments, including successful runs and recovery, still need to be completed.

## Part G: Recommendation

**To: Sameer, CTO**

I recommend ELT as the main NammaMart path, with Finance consuming only the validated marts. The raw layer preserves every source row for investigation, while staging applies the agreed rules and masks customer contact details. In local checks, ETL and ELT produced the same September delivered revenue (**950,825.60**), the same current total (**1,020,610.65**), and the same 610 payment-reconciliation rows. That gives us a useful parity check, not proof of production readiness.

ELT is easier to revise because transformations are SQL and the raw files remain available for replay. ETL still provides a clear pre-load quarantine path and is useful as an independent comparison for Finance. I have not benchmarked either design at 1,000 times the current volume or measured cloud costs; DuckDB laptop results do not establish future performance or cost.

The main risks are:

1. Business rules, including payment and delivery thresholds, still need approval from Finance and Operations.
2. Airflow scheduling, retries, recovery, and alerts have not been demonstrated by a green Airflow run.
3. Raw customer data requires strict access and retention controls, and mart-to-source lineage needs a durable row-level link.

Submission note: the configured Git remote is the course repository, not a personal fork, so I have not pushed these changes there. Airflow screenshots remain pending a successful UI run.
