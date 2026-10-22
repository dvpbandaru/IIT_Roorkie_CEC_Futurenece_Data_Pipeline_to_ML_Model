from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "include") not in sys.path:
    sys.path.insert(0, str(ROOT / "include"))

from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task

from nammamart_pipeline_utils import (
    DATA_DIR,
    QUARANTINE_DIR,
    WAREHOUSE_DB,
    append_dq_results,
    ensure_dq_table,
    mask_email,
    mask_phone,
    normalize_loyalty,
    normalize_order_status,
    normalize_payment_mode,
    record_pipeline_failure,
    validate_file,
    write_quarantine_csv,
)

DEFAULT_ARGS = {
    "owner": "prasad",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(WAREHOUSE_DB))


def _write_run_audit(con, run_id: str, files: list[dict], dag_id: str) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR,
            dag_id VARCHAR,
            status VARCHAR,
            rows_extracted BIGINT,
            rows_loaded BIGINT,
            rows_quarantined BIGINT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            failed_task VARCHAR,
            failure_reason VARCHAR
        )
        """
    )
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS rows_extracted BIGINT")
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS rows_loaded BIGINT")
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS start_time TIMESTAMP")
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS end_time TIMESTAMP")
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS failed_task VARCHAR")
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS failure_reason VARCHAR")
    rows_read = sum(item["rows_read"] for item in files)
    rows_passed = sum(item["rows_passed"] for item in files)
    rows_quarantined = sum(item["rows_quarantined"] for item in files)
    con.execute(
        "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
        [run_id, dag_id],
    )
    con.execute(
        """
        INSERT INTO pipeline_runs
        (run_id, dag_id, status, rows_extracted, rows_loaded, rows_quarantined,
         start_time, end_time, failed_task, failure_reason)
        VALUES (?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp, NULL, NULL)
        """,
        [
            run_id,
            dag_id,
            "success_with_rejections" if rows_quarantined else "success",
            rows_read,
            rows_passed,
            rows_quarantined,
        ],
    )


@dag(
    dag_id="nammamart_etl_prasad",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=record_pipeline_failure,
    tags=["nammamart", "etl", "finance", "dq"],
)
def nammamart_etl_prasad():

    @task
    def extract_orders():
        return str(DATA_DIR / "orders.csv")

    @task
    def extract_payments():
        return str(DATA_DIR / "payments.csv")

    @task
    def extract_customers():
        return str(DATA_DIR / "customers.csv")

    @task
    def extract_products():
        return str(DATA_DIR / "products.csv")

    @task
    def extract_stores():
        return str(DATA_DIR / "stores.csv")

    @task
    def extract_increment():
        return str(DATA_DIR / "orders_2026-10-01_increment.csv")

    @task
    def create_run_id():
        import re

        from airflow.operators.python import get_current_context

        return re.sub(r"[^A-Za-z0-9_-]", "_", get_current_context()["run_id"])

    @task
    def validate_orders(file_path: str, run_id: str, source_file: str = "orders.csv"):
        df = pd.read_csv(file_path)
        passed, failed, dq_rows = validate_file(df, "orders", "nammamart_etl_prasad", run_id)
        for key, master_file in [
            ("customer_id", "customers.csv"),
            ("product_id", "products.csv"),
            ("store_id", "stores.csv"),
        ]:
            master = pd.read_csv(DATA_DIR / master_file)
            master_type = {
                "customer_id": "customers",
                "product_id": "products",
                "store_id": "stores",
            }[key]
            valid_master, _, _ = validate_file(
                master, master_type, "nammamart_etl_prasad", run_id
            )
            if key == "product_id":
                product_mrp = dict(
                    zip(
                        valid_master["product_id"].astype(str).str.strip(),
                        pd.to_numeric(valid_master["mrp"], errors="coerce"),
                    )
                )
            valid_ids = set(valid_master[key].dropna().astype(str).str.strip())
            missing = passed[key].isna() | ~passed[key].astype(str).str.strip().isin(valid_ids)
            count = int(missing.sum())
            dq_rows.append({
                "run_id": run_id,
                "dag_id": "nammamart_etl_prasad",
                "check_name": f"order_{key}_referential_integrity",
                "dimension": "Consistency",
                "severity": "Critical",
                "rows_checked": len(passed),
                "rows_failed": count,
                "status": "fail" if count else "pass",
                "checked_at": pendulum.now("Asia/Kolkata").to_iso8601_string(),
            })
            if count:
                failed_rows = passed.loc[missing].copy()
                failed_rows["failure_reason"] = f"{key} not found in {master_file}"
                failed = pd.concat([failed, failed_rows])
                passed = passed.loc[~missing].copy()
        product_prices = df["product_id"].astype(str).str.strip().map(product_mrp)
        high_prices = (
            pd.to_numeric(df["unit_price"], errors="coerce") > product_prices * 10
        )
        dq_rows.append({
            "run_id": run_id,
            "dag_id": "nammamart_etl_prasad",
            "check_name": f"unit_price_distribution_vs_mrp_{Path(source_file).stem}",
            "dimension": "Accuracy",
            "severity": "Warning",
            "rows_checked": len(df),
            "rows_failed": int(high_prices.sum()),
            "status": "warn" if high_prices.any() else "pass",
            "checked_at": pendulum.now("Asia/Kolkata").to_iso8601_string(),
        })
        if source_file == "orders.csv":
            with connection() as con:
                ensure_dq_table(con)
                previous = con.execute(
                    """
                    SELECT rows_checked
                    FROM dq_results
                    WHERE dag_id = 'nammamart_etl_prasad'
                      AND check_name = 'order_volume_vs_previous_run'
                      AND run_id <> ?
                    ORDER BY checked_at DESC
                    LIMIT 1
                    """,
                    [run_id],
                ).fetchone()
            volume_failed = int(previous is not None and len(df) < previous[0] * 0.1)
            dq_rows.append({
                "run_id": run_id,
                "dag_id": "nammamart_etl_prasad",
                "check_name": "order_volume_vs_previous_run",
                "dimension": "Volume",
                "severity": "Warning",
                "rows_checked": len(df),
                "rows_failed": volume_failed,
                "status": "warn" if volume_failed else "pass",
                "checked_at": pendulum.now("Asia/Kolkata").to_iso8601_string(),
            })
        file_stem = Path(source_file).stem
        passed_path = str(Path(QUARANTINE_DIR) / f"{file_stem}_passed_{run_id}.csv")
        failed_path = write_quarantine_csv(failed, source_file, run_id, "nammamart_etl_prasad")
        passed.to_csv(passed_path, index=False)
        return {"source_file": source_file, "passed_csv": passed_path, "failed_csv": failed_path, "run_id": run_id, "dq_rows": dq_rows, "rows_read": len(df), "rows_passed": len(passed), "rows_quarantined": len(failed)}

    @task
    def validate_payments(file_path: str, run_id: str):
        df = pd.read_csv(file_path)
        passed, failed, dq_rows = validate_file(df, "payments", "nammamart_etl_prasad", run_id)
        passed_path = str(Path(QUARANTINE_DIR) / f"payments_passed_{run_id}.csv")
        failed_path = write_quarantine_csv(failed, "payments", run_id, "nammamart_etl_prasad")
        passed.to_csv(passed_path, index=False)
        return {"source_file": "payments.csv", "passed_csv": passed_path, "failed_csv": failed_path, "run_id": run_id, "dq_rows": dq_rows, "rows_read": len(df), "rows_passed": len(passed), "rows_quarantined": len(failed)}

    @task
    def validate_customers(file_path: str, run_id: str):
        df = pd.read_csv(file_path)
        passed, failed, dq_rows = validate_file(df, "customers", "nammamart_etl_prasad", run_id)
        passed_path = str(Path(QUARANTINE_DIR) / f"customers_passed_{run_id}.csv")
        failed_path = write_quarantine_csv(failed, "customers", run_id, "nammamart_etl_prasad")
        safe_passed = passed.copy()
        safe_passed["full_name"] = safe_passed["customer_id"].map(lambda value: f"Customer {value}")
        safe_passed["email"] = safe_passed["email"].map(mask_email)
        safe_passed["phone"] = safe_passed["phone"].map(mask_phone)
        safe_passed.to_csv(passed_path, index=False)
        return {"source_file": "customers.csv", "passed_csv": passed_path, "failed_csv": failed_path, "run_id": run_id, "dq_rows": dq_rows, "rows_read": len(df), "rows_passed": len(passed), "rows_quarantined": len(failed)}

    @task
    def validate_products(file_path: str, run_id: str):
        df = pd.read_csv(file_path)
        passed, failed, dq_rows = validate_file(df, "products", "nammamart_etl_prasad", run_id)
        passed_path = str(Path(QUARANTINE_DIR) / f"products_passed_{run_id}.csv")
        failed_path = write_quarantine_csv(failed, "products", run_id, "nammamart_etl_prasad")
        passed.to_csv(passed_path, index=False)
        return {"source_file": "products.csv", "passed_csv": passed_path, "failed_csv": failed_path, "run_id": run_id, "dq_rows": dq_rows, "rows_read": len(df), "rows_passed": len(passed), "rows_quarantined": len(failed)}

    @task
    def validate_stores(file_path: str, run_id: str):
        df = pd.read_csv(file_path)
        passed, failed, dq_rows = validate_file(df, "stores", "nammamart_etl_prasad", run_id)
        passed_path = str(Path(QUARANTINE_DIR) / f"stores_passed_{run_id}.csv")
        failed_path = write_quarantine_csv(failed, "stores", run_id, "nammamart_etl_prasad")
        passed.to_csv(passed_path, index=False)
        return {"source_file": "stores.csv", "passed_csv": passed_path, "failed_csv": failed_path, "run_id": run_id, "dq_rows": dq_rows, "rows_read": len(df), "rows_passed": len(passed), "rows_quarantined": len(failed)}

    @task
    def transform_and_load(orders_result, increment_result, payments_result, customers_result, products_result, stores_result):
        con = connection()
        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_customer (customer_id VARCHAR, full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR, loyalty_tier VARCHAR)")
        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_product (product_id VARCHAR, product_name VARCHAR, category VARCHAR, brand VARCHAR, cost_price DOUBLE)")
        con.execute("CREATE TABLE IF NOT EXISTS etl_dim_store (store_id VARCHAR, store_name VARCHAR, zone VARCHAR, capacity_orders_per_day INTEGER)")
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders (order_id VARCHAR, order_ts TIMESTAMP, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR, quantity INTEGER, order_status VARCHAR, payment_mode VARCHAR, order_amount DOUBLE, net_revenue DOUBLE, gross_margin DOUBLE, zone VARCHAR, category VARCHAR, order_date DATE, delivery_minutes DOUBLE, within_15_min BOOLEAN)")
        con.execute("ALTER TABLE etl_fact_orders ADD COLUMN IF NOT EXISTS delivery_minutes DOUBLE")

        orders = pd.read_csv(orders_result["passed_csv"])
        increment = pd.read_csv(increment_result["passed_csv"])
        all_increment = pd.read_csv(DATA_DIR / "orders_2026-10-01_increment.csv")
        source_order_ids = set(pd.read_csv(DATA_DIR / "orders.csv")["order_id"])
        updated_count = int(increment["order_id"].isin(source_order_ids).sum())
        inserted_count = int((~increment["order_id"].isin(source_order_ids)).sum())
        orders = orders.loc[~orders["order_id"].isin(increment["order_id"])]
        orders = pd.concat([orders, increment], ignore_index=True)
        payments = pd.read_csv(payments_result["passed_csv"])
        customers = pd.read_csv(customers_result["passed_csv"])
        products = pd.read_csv(products_result["passed_csv"])
        stores = pd.read_csv(stores_result["passed_csv"])

        orders["order_status"] = orders["order_status"].apply(normalize_order_status)
        orders["payment_mode"] = orders["payment_mode"].apply(normalize_payment_mode)
        orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce")
        customers["email"] = customers["email"].apply(mask_email)
        customers["phone"] = customers["phone"].apply(mask_phone)
        customers["loyalty_tier"] = customers["loyalty_tier"].apply(normalize_loyalty)
        customers = customers.drop_duplicates(subset=["customer_id"], keep="last")
        products = products.drop_duplicates(subset=["product_id"], keep="last")
        stores = stores.drop_duplicates(subset=["store_id"], keep="last")

        fact = orders.merge(customers[["customer_id", "full_name", "email", "phone", "loyalty_tier"]], on="customer_id", how="left")
        fact = fact.merge(products[["product_id", "product_name", "category", "brand", "cost_price"]], on="product_id", how="left")
        fact = fact.merge(stores[["store_id", "store_name", "zone", "capacity_orders_per_day"]], on="store_id", how="left")
        fact["order_date"] = pd.to_datetime(fact["order_ts"]).dt.date
        fact["net_revenue"] = fact.apply(lambda row: row["order_amount"] if row["order_status"] == "DELIVERED" else 0.0, axis=1)
        fact["gross_margin"] = fact["net_revenue"] - (fact["quantity"] * fact["cost_price"])
        fact["delivery_minutes"] = pd.to_numeric(fact["delivery_minutes"], errors="coerce")
        fact["within_15_min"] = (
            fact["order_status"].eq("DELIVERED")
            & pd.to_numeric(fact["delivery_minutes"], errors="coerce").le(15)
        )

        customer_dim = customers[["customer_id", "full_name", "email", "phone", "city", "loyalty_tier"]].copy()
        product_dim = products[["product_id", "product_name", "category", "brand", "cost_price"]].copy()
        store_dim = stores[["store_id", "store_name", "zone", "capacity_orders_per_day"]].copy()

        con.execute("DELETE FROM etl_dim_customer")
        con.execute("DELETE FROM etl_dim_product")
        con.execute("DELETE FROM etl_dim_store")
        con.execute("DELETE FROM etl_fact_orders")
        con.register("customer_dim_df", customer_dim)
        con.register("product_dim_df", product_dim)
        con.register("store_dim_df", store_dim)
        con.register("fact_df", fact)
        con.execute("INSERT INTO etl_dim_customer SELECT * FROM customer_dim_df")
        con.execute("INSERT INTO etl_dim_product SELECT * FROM product_dim_df")
        con.execute("INSERT INTO etl_dim_store SELECT * FROM store_dim_df")
        con.execute(
            "INSERT INTO etl_fact_orders (order_id, order_ts, customer_id, store_id, product_id, quantity, order_status, payment_mode, order_amount, net_revenue, gross_margin, zone, category, order_date, delivery_minutes, within_15_min) SELECT order_id, order_ts, customer_id, store_id, product_id, quantity, order_status, payment_mode, order_amount, net_revenue, gross_margin, zone, category, order_date, delivery_minutes, within_15_min FROM fact_df"
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE etl_increment_audit (
                run_id VARCHAR, rows_updated INTEGER, rows_inserted INTEGER
            )
            """,
        )
        con.execute(
            "INSERT INTO etl_increment_audit VALUES (?, ?, ?)",
            [orders_result["run_id"], updated_count, inserted_count],
        )
        con.close()
        return {"orders": len(fact), "customers": len(customer_dim), "products": len(product_dim), "stores": len(store_dim), "increment_updated": updated_count, "increment_inserted": inserted_count}

    @task
    def reconcile_payments(transform_result, orders_result, increment_result, payments_result):
        con = connection()
        orders = pd.read_csv(orders_result["passed_csv"])
        increment = pd.read_csv(increment_result["passed_csv"])
        orders = orders.loc[~orders["order_id"].isin(increment["order_id"])]
        orders = pd.concat([orders, increment], ignore_index=True)
        payments = pd.read_csv(payments_result["passed_csv"])
        payments = payments[
            payments["payment_status"].astype(str).str.strip().str.upper().eq("SUCCESS")
        ].copy()
        payment_totals = payments.groupby("order_id", as_index=False)["amount_paid"].sum().rename(columns={"amount_paid": "total_paid"})
        order_summary = orders[["order_id", "order_amount", "order_status"]].copy()
        mismatches = order_summary.merge(payment_totals, on="order_id", how="outer")
        mismatches = mismatches[
            (
                mismatches["order_status"].astype(str).str.upper().eq("DELIVERED")
                & (
                    mismatches["total_paid"].isna()
                    | ((mismatches["order_amount"] - mismatches["total_paid"]).abs() > 0.01)
                )
            )
            | (
                mismatches["order_status"].astype(str).str.upper().isin(["CANCELLED", "RETURNED"])
                & mismatches["total_paid"].notna()
            )
            | (mismatches["order_status"].isna())
            | (
                mismatches["total_paid"].notna()
                & ((mismatches["order_amount"] - mismatches["total_paid"]).abs() > 0.01)
            )
        ].copy()
        mismatches["reason"] = "amount mismatch"
        mismatches.loc[mismatches["total_paid"].isna(), "reason"] = "successful payment missing"
        mismatches.loc[
            mismatches["order_status"].astype(str).str.upper().isin(["CANCELLED", "RETURNED"])
            & mismatches["total_paid"].notna(),
            "reason",
        ] = "payment exists for non-delivered order"
        mismatches.loc[mismatches["order_status"].isna(), "reason"] = "payment has no matching order"
        mismatches = mismatches[["order_id", "order_amount", "total_paid", "reason"]]
        con.execute("CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (order_id VARCHAR, order_amount DOUBLE, total_paid DOUBLE, reason VARCHAR)")
        con.execute("DELETE FROM etl_payment_reconciliation")
        con.register("mismatch_df", mismatches)
        con.execute("INSERT INTO etl_payment_reconciliation SELECT * FROM mismatch_df")
        print(con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone())
        con.close()
        run_id = orders_result["run_id"]
        return {
            "mismatches": mismatches.to_dict(orient="records"),
            "dq_rows": [{
                "run_id": run_id,
                "dag_id": "nammamart_etl_prasad",
                "check_name": "delivered_orders_reconcile_to_success_payments",
                "dimension": "Consistency",
                "severity": "Critical",
                "rows_checked": len(orders),
                "rows_failed": len(mismatches),
                "status": "fail" if len(mismatches) else "pass",
                "checked_at": pendulum.now("Asia/Kolkata").to_iso8601_string(),
            }],
        }

    @task
    def build_reports(transform_result, reconciliation_result, validation_results):
        con = connection()
        con.execute("CREATE OR REPLACE TABLE etl_daily_zone_revenue AS SELECT zone, store_id, CAST(order_date AS DATE) AS order_date, SUM(net_revenue) AS revenue FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY zone, store_id, CAST(order_date AS DATE)")
        con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT category, SUM(gross_margin) AS gross_margin, SUM(net_revenue) AS revenue FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY category")
        zone_df = con.execute("SELECT * FROM etl_daily_zone_revenue ORDER BY order_date, zone").fetchdf()
        category_df = con.execute("SELECT * FROM etl_category_margin ORDER BY gross_margin DESC").fetchdf()
        ensure_dq_table(con)
        dq_rows = [row for result in validation_results for row in result["dq_rows"]]
        dq_rows.extend(reconciliation_result["dq_rows"])
        append_dq_results(con, dq_rows)
        _write_run_audit(con, validation_results[0]["run_id"], validation_results, "nammamart_etl_prasad")
        con.execute("CREATE OR REPLACE TABLE etl_daily_store_kpis AS SELECT store_id, zone, order_date, SUM(revenue) AS net_revenue FROM etl_daily_zone_revenue GROUP BY store_id, zone, order_date")
        con.execute("CREATE OR REPLACE TABLE etl_delivery_sla AS SELECT store_id, AVG(delivery_minutes) AS avg_delivery_minutes, 100.0 * AVG(CASE WHEN within_15_min THEN 1.0 ELSE 0.0 END) AS pct_within_15_min FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY store_id")
        con.execute("CREATE OR REPLACE TABLE etl_payment_mode_revenue AS SELECT payment_mode, SUM(net_revenue) AS revenue FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY payment_mode")
        con.execute("CREATE OR REPLACE TABLE etl_top_customers AS SELECT f.customer_id, c.full_name, c.loyalty_tier, SUM(f.net_revenue) AS total_spend FROM etl_fact_orders f LEFT JOIN etl_dim_customer c ON c.customer_id=f.customer_id WHERE f.order_status='DELIVERED' GROUP BY f.customer_id, c.full_name, c.loyalty_tier ORDER BY total_spend DESC LIMIT 10")
        failed_frames = []
        for result in validation_results:
            failed = pd.read_csv(result["failed_csv"])
            if not failed.empty:
                failed["source_file"] = result["source_file"]
                failed_frames.append(failed[["source_file", "failure_reason"]])
        rejected = (
            pd.concat(failed_frames, ignore_index=True)
            .groupby(["source_file", "failure_reason"], dropna=False)
            .size()
            .reset_index(name="rejected_rows")
            if failed_frames
            else pd.DataFrame(columns=["source_file", "failure_reason", "rejected_rows"])
        )
        con.execute("CREATE OR REPLACE TABLE etl_rejected_by_reason (source_file VARCHAR, failure_reason VARCHAR, rejected_rows BIGINT)")
        con.register("etl_rejected_df", rejected)
        con.execute("INSERT INTO etl_rejected_by_reason SELECT * FROM etl_rejected_df")
        print(zone_df.to_string(index=False))
        print(category_df.to_string(index=False))
        con.close()
        return {"zone_revenue": zone_df.to_dict(orient="records"), "category_margin": category_df.to_dict(orient="records")}

    orders = extract_orders()
    increment = extract_increment()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
    run_id = create_run_id()

    order_res = validate_orders(orders, run_id)
    increment_res = validate_orders(
        increment, run_id, "orders_2026-10-01_increment.csv"
    )
    payment_res = validate_payments(payments, run_id)
    customer_res = validate_customers(customers, run_id)
    product_res = validate_products(products, run_id)
    store_res = validate_stores(stores, run_id)

    transform = transform_and_load(order_res, increment_res, payment_res, customer_res, product_res, store_res)
    reconciliation = reconcile_payments(transform, order_res, increment_res, payment_res)
    reports = build_reports(
        transform, reconciliation, [order_res, increment_res, payment_res, customer_res, product_res, store_res]
    )

    return transform, reconciliation, reports


nammamart_etl_prasad_dag = nammamart_etl_prasad()
