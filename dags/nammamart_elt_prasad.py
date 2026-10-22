from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for import_path in (ROOT, ROOT / "include"):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))

import duckdb
import pendulum
from airflow.decorators import dag, task

from nammamart_pipeline_utils import DATA_DIR, WAREHOUSE_DB, record_pipeline_failure

SQL_DIR = ROOT / "include" / "sql" / "elt"
SOURCES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

DEFAULT_ARGS = {
    "owner": "prasad",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def execute_sql(sql_name: str) -> None:
    with duckdb.connect(str(WAREHOUSE_DB)) as con:
        con.execute((SQL_DIR / sql_name).read_text())


@dag(
    dag_id="nammamart_elt_prasad",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=record_pipeline_failure,
    tags=["nammamart", "elt", "analytics", "dq"],
)
def nammamart_elt_prasad():
    @task
    def extract_orders() -> str:
        return str(DATA_DIR / SOURCES["orders"])

    @task
    def extract_payments() -> str:
        return str(DATA_DIR / SOURCES["payments"])

    @task
    def extract_customers() -> str:
        return str(DATA_DIR / SOURCES["customers"])

    @task
    def extract_products() -> str:
        return str(DATA_DIR / SOURCES["products"])

    @task
    def extract_stores() -> str:
        return str(DATA_DIR / SOURCES["stores"])

    @task
    def load_raw(orders: str, payments: str, customers: str, products: str, stores: str) -> str:
        import csv

        from airflow.operators.python import get_current_context

        run_id = get_current_context()["run_id"]
        with duckdb.connect(str(WAREHOUSE_DB)) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS dq_results (
                    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR,
                    dimension VARCHAR, severity VARCHAR, rows_checked BIGINT,
                    rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR, dag_id VARCHAR, status VARCHAR,
                    rows_extracted BIGINT, rows_loaded BIGINT,
                    rows_quarantined BIGINT, start_time TIMESTAMP,
                    end_time TIMESTAMP, failed_task VARCHAR,
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
            for table, file_name, path in [
                ("orders", "orders.csv", orders),
                ("payments", "payments.csv", payments),
                ("customers", "customers.csv", customers),
                ("products", "products.csv", products),
                ("stores", "stores.csv", stores),
            ]:
                with open(path, newline="", encoding="utf-8") as source:
                    headers = next(csv.reader(source))
                columns = ", ".join(f'"{column}" VARCHAR' for column in headers)
                raw_table = f"raw_{table}"
                con.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {raw_table} (
                        {columns},
                        _loaded_at TIMESTAMP,
                        _source_file VARCHAR,
                        _run_id VARCHAR
                    )
                    """
                )
                con.execute(
                    f"""
                    INSERT INTO {raw_table}
                    SELECT *, current_timestamp, ?, ?
                    FROM read_csv(?, header = true, all_varchar = true)
                    """,
                    [file_name, run_id, path],
                )
        return run_id

    @task
    def build_staging(raw_run_id: str) -> str:
        for sql_name in [
            "stg_customers.sql",
            "stg_products.sql",
            "stg_stores.sql",
            "stg_orders.sql",
            "stg_payments.sql",
        ]:
            execute_sql(sql_name)
        return raw_run_id

    @task
    def load_increment(run_id: str) -> str:
        increment_path = DATA_DIR / "orders_2026-10-01_increment.csv"
        with duckdb.connect(str(WAREHOUSE_DB)) as con:
            con.execute(
                """
                INSERT INTO raw_orders
                SELECT *, current_timestamp, 'orders_2026-10-01_increment.csv', ?
                FROM read_csv(?, header = true, all_varchar = true)
                """,
                [run_id, str(increment_path)],
            )
        return run_id

    @task
    def merge_increment(run_id: str) -> str:
        execute_sql("merge_increment_orders.sql")
        return run_id

    @task
    def build_marts(run_id: str) -> str:
        for sql_name in [
            "mart_daily_store_kpis.sql",
            "mart_category_margin.sql",
            "mart_delivery_sla.sql",
            "mart_payment_reconciliation.sql",
            "mart_top_customers.sql",
            "mart_payment_mode_revenue.sql",
            "mart_rejected_by_reason.sql",
            "record_dq_and_audit.sql",
        ]:
            execute_sql(sql_name)
        with duckdb.connect(str(WAREHOUSE_DB), read_only=True) as con:
            print("Daily store KPIs")
            print(con.execute("SELECT * FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id").fetchdf().to_string(index=False))
            print("Category margin")
            print(con.execute("SELECT * FROM mart_category_margin ORDER BY category").fetchdf().to_string(index=False))
        return run_id

    source_paths = [
        extract_orders(),
        extract_payments(),
        extract_customers(),
        extract_products(),
        extract_stores(),
    ]
    raw_run_id = load_raw(*source_paths)
    staging_run_id = build_staging(raw_run_id)
    increment_run_id = load_increment(staging_run_id)
    merged_run_id = merge_increment(increment_run_id)
    build_marts(merged_run_id)


nammamart_elt_prasad_dag = nammamart_elt_prasad()
