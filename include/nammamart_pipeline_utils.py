from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pendulum

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "include" / "data" / "nammamart"
WAREHOUSE_DB = ROOT / "include" / "warehouse_prasad.duckdb"
QUARANTINE_DIR = ROOT / "include" / "quarantine" / "prasad"

ALLOWED_ORDER_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAYMENT_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}


def normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_order_status(value: Any) -> str:
    text = normalize_text(value).upper().replace(" ", "")
    mapping = {
        "DELIVERED": "DELIVERED",
        "CANCELLED": "CANCELLED",
        "RETURNED": "RETURNED",
    }
    return mapping.get(text, text)


def normalize_payment_mode(value: Any) -> str:
    text = normalize_text(value).upper().replace(" ", "")
    mapping = {
        "UPI": "UPI",
        "CARD": "Card",
        "CASHONDELIVERY": "Cash on Delivery",
        "COD": "Cash on Delivery",
        "WALLET": "Wallet",
    }
    if text in {"UPI", "CARD", "CASHONDELIVERY", "COD", "WALLET"}:
        return mapping[text]
    return normalize_text(value)


def normalize_loyalty(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return text
    return text.title() if text.lower() not in {"silver", "gold", "platinum", "bronze"} else text.title()


def mask_email(value: Any) -> str:
    text = normalize_text(value)
    if not text or re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text) is None:
        return "masked@invalid.local"
    local, domain = text.split("@", 1)
    if "*" in local:
        return text
    masked_local = local[:3] + ("*" * max(len(local) - 3, 3))
    return f"{masked_local}@{domain}"


def mask_phone(value: Any) -> str:
    text = normalize_text(value)
    if "*" in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 10:
        return digits[:2] + ("*" * (len(digits) - 4)) + digits[-2:]
    return "**********" if digits else "**********"


def coerce_order_amount(row: pd.Series) -> float:
    try:
        qty = float(row["quantity"])
        unit_price = float(row["unit_price"])
        discount_pct = float(row["discount_pct"])
        return round(qty * unit_price * (1 - discount_pct / 100), 2)
    except Exception:
        return float("nan")


def ensure_dq_table(connection: duckdb.DuckDBPyConnection, table_name: str = "dq_results") -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id VARCHAR,
            dag_id VARCHAR,
            check_name VARCHAR,
            dimension VARCHAR,
            severity VARCHAR,
            rows_checked INTEGER,
            rows_failed INTEGER,
            status VARCHAR,
            checked_at TIMESTAMP
        )
        """
    )


def append_dq_results(connection: duckdb.DuckDBPyConnection, records: list[dict], table_name: str = "dq_results") -> None:
    if not records:
        return
    ensure_dq_table(connection, table_name)
    connection.execute(
        f"DELETE FROM {table_name} WHERE dag_id = ? AND run_id = ?",
        [records[0]["dag_id"], records[0]["run_id"]],
    )
    rows = [
        (
            r.get("run_id"),
            r.get("dag_id"),
            r.get("check_name"),
            r.get("dimension"),
            r.get("severity"),
            int(r.get("rows_checked", 0)),
            int(r.get("rows_failed", 0)),
            r.get("status"),
            pd.Timestamp(r.get("checked_at")).to_pydatetime(),
        )
        for r in records
    ]
    connection.executemany(
        f"""
        INSERT INTO {table_name} (
            run_id, dag_id, check_name, dimension, severity,
            rows_checked, rows_failed, status, checked_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def record_pipeline_failure(context: dict) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    task_instance = context.get("task_instance")
    failed_task = task_instance.task_id if task_instance else "unknown"
    exception = context.get("exception")
    reason = str(exception) if exception else "Task failure"
    with duckdb.connect(str(WAREHOUSE_DB)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR, dag_id VARCHAR, status VARCHAR,
                rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT,
                start_time TIMESTAMP, end_time TIMESTAMP,
                failed_task VARCHAR, failure_reason VARCHAR
            )
            """
        )
        con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS rows_extracted BIGINT")
        con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS rows_loaded BIGINT")
        con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS start_time TIMESTAMP")
        con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS end_time TIMESTAMP")
        con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS failed_task VARCHAR")
        con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS failure_reason VARCHAR")
        con.execute(
            "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
            [run_id, dag_id],
        )
        con.execute(
            """
            INSERT INTO pipeline_runs
            (run_id, dag_id, status, rows_extracted, rows_loaded, rows_quarantined,
             start_time, end_time, failed_task, failure_reason)
            VALUES (?, ?, 'failed', 0, 0, 0, current_timestamp, current_timestamp, ?, ?)
            """,
            [run_id, dag_id, failed_task, reason],
        )


def write_quarantine_csv(df: pd.DataFrame, file_name: str, run_id: str, dag_id: str = "nammamart") -> str:
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    target = QUARANTINE_DIR / f"{file_name.replace('.csv', '')}_failed_{run_id}.csv"
    safe_df = df.copy()
    if file_name.startswith("customers"):
        for column in ("email", "phone"):
            if column in safe_df:
                safe_df[column] = safe_df[column].map(
                    mask_email if column == "email" else mask_phone
                )
        if "full_name" in safe_df:
            safe_df["full_name"] = "[MASKED]"
    safe_df.to_csv(target, index=False)
    return str(target)


def validate_file(df: pd.DataFrame, type_name: str, dag_id: str, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    dq_results: list[dict] = []
    checked_at = pendulum.now("Asia/Kolkata").to_iso8601_string()
    failed_index: set[int] = set()
    failure_map: dict[int, list[str]] = {}

    def note(check_name: str, dimension: str, severity: str, rows_checked: int, failed_count: int) -> None:
        dq_results.append(
            {
                "run_id": run_id,
                "dag_id": dag_id,
                "check_name": check_name,
                "dimension": dimension,
                "severity": severity,
                "rows_checked": rows_checked,
                "rows_failed": failed_count,
                "status": (
                    "pass"
                    if failed_count == 0
                    else "warn"
                    if severity == "Warning"
                    else "fail"
                ),
                "checked_at": checked_at,
            }
        )

    if type_name == "orders":
        rows = len(df)
        duplicate_ids = df[df["order_id"].duplicated(keep=False)].index
        failed_index.update(duplicate_ids)
        for idx in duplicate_ids:
            failure_map.setdefault(int(idx), []).append("duplicate order_id")
        note("unique_order_id", "Uniqueness", "Critical", rows, len(duplicate_ids))

        parsed_order_ts = pd.to_datetime(df["order_ts"], errors="coerce", format="mixed")
        invalid_order_ts = df[parsed_order_ts.isna()].index
        failed_index.update(invalid_order_ts)
        for idx in invalid_order_ts:
            failure_map.setdefault(int(idx), []).append("order_ts is missing or invalid")
        note("order_timestamp_present", "Completeness", "Critical", rows, len(invalid_order_ts))

        future_order_ts = df[parsed_order_ts > pd.Timestamp.now()].index
        failed_index.update(future_order_ts)
        for idx in future_order_ts:
            failure_map.setdefault(int(idx), []).append("order_ts is in the future")
        note("order_timestamp_not_future", "Timeliness", "Critical", rows, len(future_order_ts))
        latest_order = parsed_order_ts.max()
        stale = int(pd.notna(latest_order) and latest_order < pd.Timestamp.now() - pd.Timedelta(days=1))
        note("latest_order_freshness", "Timeliness", "Warning", rows, stale)

        bad_status = df[~df["order_status"].fillna("").astype(str).str.strip().str.upper().isin({"DELIVERED", "CANCELLED", "RETURNED"})].index
        failed_index.update(bad_status)
        for idx in bad_status:
            failure_map.setdefault(int(idx), []).append("order_status not in allowed list")
        note("valid_order_status", "Validity", "Critical", rows, len(bad_status))

        missing_delivery = df[(df["order_status"].fillna("").astype(str).str.upper() == "DELIVERED") & df["delivery_minutes"].isna()].index
        failed_index.update(missing_delivery)
        for idx in missing_delivery:
            failure_map.setdefault(int(idx), []).append("DELIVERED order missing delivery_minutes")
        note("delivered_delivery_minutes", "Completeness", "Critical", rows, len(missing_delivery))
        delivery = pd.to_numeric(df["delivery_minutes"], errors="coerce")
        implausible_delivery = df[delivery > 120].index
        failed_index.update(implausible_delivery)
        for idx in implausible_delivery:
            failure_map.setdefault(int(idx), []).append("delivery_minutes exceeds 120")
        note("delivery_under_120_minutes", "Accuracy", "Critical", rows, len(implausible_delivery))

        quantities = pd.to_numeric(df["quantity"], errors="coerce")
        bad_quantity = df[quantities.isna() | (quantities <= 0)].index
        failed_index.update(bad_quantity)
        for idx in bad_quantity:
            failure_map.setdefault(int(idx), []).append("quantity must be > 0")
        note("positive_quantity", "Accuracy", "Critical", rows, len(bad_quantity))

        amt_formula = df.apply(coerce_order_amount, axis=1)
        supplied_amount = pd.to_numeric(df["order_amount"], errors="coerce")
        order_mismatch = df[
            supplied_amount.isna() | amt_formula.isna()
            | ((supplied_amount - amt_formula).abs() > 0.01)
        ].index
        failed_index.update(order_mismatch)
        for idx in order_mismatch:
            failure_map.setdefault(int(idx), []).append("order_amount mismatch with formula")
        note("order_amount_formula", "Accuracy", "Critical", rows, len(order_mismatch))
        unit_prices = pd.to_numeric(df["unit_price"], errors="coerce")
        invalid_unit_price = df[unit_prices.isna() | (unit_prices <= 0)].index
        failed_index.update(invalid_unit_price)
        for idx in invalid_unit_price:
            failure_map.setdefault(int(idx), []).append("unit_price must be positive")
        note("positive_unit_price", "Accuracy", "Critical", rows, len(invalid_unit_price))

        normalized_modes = df["payment_mode"].apply(normalize_payment_mode)
        invalid_payment_mode = df[~normalized_modes.isin(ALLOWED_PAYMENT_MODES)].index
        failed_index.update(invalid_payment_mode)
        for idx in invalid_payment_mode:
            failure_map.setdefault(int(idx), []).append("payment_mode invalid")
        note("valid_payment_mode", "Validity", "Critical", rows, len(invalid_payment_mode))

        unknown_customer = df[df["customer_id"].isna() | (df["customer_id"].astype(str).str.strip() == "")].index
        failed_index.update(unknown_customer)
        for idx in unknown_customer:
            failure_map.setdefault(int(idx), []).append("customer_id missing")
        note("customer_id_present", "Completeness", "Critical", rows, len(unknown_customer))
        missing_order_key = df[df["order_id"].isna() | (df["order_id"].astype(str).str.strip() == "")].index
        failed_index.update(missing_order_key)
        for idx in missing_order_key:
            failure_map.setdefault(int(idx), []).append("order_id missing")
        note("order_id_present", "Completeness", "Critical", rows, len(missing_order_key))
        bad_discount = df[~pd.to_numeric(df["discount_pct"], errors="coerce").between(0, 100)].index
        failed_index.update(bad_discount)
        for idx in bad_discount:
            failure_map.setdefault(int(idx), []).append("discount_pct must be between 0 and 100")
        note("discount_pct_range", "Accuracy", "Critical", rows, len(bad_discount))
        missing_store_or_product = df[
            df["store_id"].isna() | df["product_id"].isna()
            | (df["store_id"].astype(str).str.strip() == "")
            | (df["product_id"].astype(str).str.strip() == "")
        ].index
        failed_index.update(missing_store_or_product)
        for idx in missing_store_or_product:
            failure_map.setdefault(int(idx), []).append("store_id or product_id missing")
        note("store_and_product_keys_present", "Completeness", "Critical", rows, len(missing_store_or_product))

    elif type_name == "payments":
        rows = len(df)
        dup_ids = df[df["payment_id"].duplicated(keep=False)].index
        failed_index.update(dup_ids)
        for idx in dup_ids:
            failure_map.setdefault(int(idx), []).append("duplicate payment_id")
        note("unique_payment_id", "Uniqueness", "Critical", rows, len(dup_ids))

        bad_status = df[~df["payment_status"].fillna("").astype(str).str.upper().isin({"SUCCESS", "PENDING", "FAILED"})].index
        failed_index.update(bad_status)
        for idx in bad_status:
            failure_map.setdefault(int(idx), []).append("invalid payment_status")
        note("valid_payment_status", "Validity", "Critical", rows, len(bad_status))

        normalized_modes = df["payment_mode"].apply(normalize_payment_mode)
        bad_modes = df[~normalized_modes.isin(ALLOWED_PAYMENT_MODES)].index
        failed_index.update(bad_modes)
        for idx in bad_modes:
            failure_map.setdefault(int(idx), []).append("invalid payment_mode")
        note("valid_payment_mode", "Validity", "Critical", rows, len(bad_modes))
        amounts = pd.to_numeric(df["amount_paid"], errors="coerce")
        bad_amounts = df[amounts.isna() | (amounts < 0)].index
        failed_index.update(bad_amounts)
        for idx in bad_amounts:
            failure_map.setdefault(int(idx), []).append("amount_paid missing or negative")
        note("payment_amount_nonnegative", "Accuracy", "Critical", rows, len(bad_amounts))
        payment_times = pd.to_datetime(df["payment_ts"], errors="coerce", format="mixed")
        bad_times = df[payment_times.isna() | (payment_times > pd.Timestamp.now())].index
        failed_index.update(bad_times)
        for idx in bad_times:
            failure_map.setdefault(int(idx), []).append("payment_ts invalid or in the future")
        note("payment_timestamp_current", "Timeliness", "Critical", rows, len(bad_times))
        missing_required = df[
            df["order_id"].isna() | (df["order_id"].astype(str).str.strip() == "")
        ].index
        failed_index.update(missing_required)
        for idx in missing_required:
            failure_map.setdefault(int(idx), []).append("order_id missing")
        note("payment_order_id_present", "Completeness", "Critical", rows, len(missing_required))
        missing_refs = df[
            (df["payment_status"].astype(str).str.upper() == "SUCCESS")
            & (df["gateway_ref"].isna() | (df["gateway_ref"].astype(str).str.strip() == ""))
        ].index
        note("successful_payment_gateway_reference", "Completeness", "Warning", rows, len(missing_refs))

    elif type_name == "customers":
        rows = len(df)
        missing_identity = df[
            df["customer_id"].isna() | (df["customer_id"].astype(str).str.strip() == "")
            | df["full_name"].isna() | (df["full_name"].astype(str).str.strip() == "")
        ].index
        failed_index.update(missing_identity)
        for idx in missing_identity:
            failure_map.setdefault(int(idx), []).append("customer_id or full_name missing")
        note("customer_identity_present", "Completeness", "Critical", rows, len(missing_identity))
        email_invalid = df[~df["email"].fillna("").astype(str).str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")].index
        note("email_format", "Validity", "Warning", rows, len(email_invalid))

        phone_digits = df["phone"].fillna("").astype(str).str.replace(r"\D", "", regex=True)
        phone_invalid = df[phone_digits.str.len() != 10].index
        note("phone_format", "Validity", "Warning", rows, len(phone_invalid))

        duplicate_ids = df[df["customer_id"].duplicated(keep=False)].index
        failed_index.update(duplicate_ids)
        for idx in duplicate_ids:
            failure_map.setdefault(int(idx), []).append("duplicate customer_id")
        note("unique_customer_id", "Uniqueness", "Critical", rows, len(duplicate_ids))

        signup_dates = pd.to_datetime(df["signup_date"], errors="coerce")
        bad_dates = df[signup_dates.isna() | (signup_dates > pd.Timestamp.now().normalize())].index
        failed_index.update(bad_dates)
        for idx in bad_dates:
            failure_map.setdefault(int(idx), []).append("signup_date invalid or in the future")
        note("signup_date_timeliness", "Timeliness", "Critical", rows, len(bad_dates))
        valid_tiers = {"BRONZE", "SILVER", "GOLD", "PLATINUM"}
        tier_invalid = df[~df["loyalty_tier"].fillna("").astype(str).str.strip().str.upper().isin(valid_tiers)].index
        note("loyalty_tier_consistent", "Consistency", "Warning", rows, len(tier_invalid))

    elif type_name == "products":
        rows = len(df)
        missing_category = df[df["category"].isna() | (df["category"].astype(str).str.strip() == "")].index
        failed_index.update(missing_category)
        for idx in missing_category:
            failure_map.setdefault(int(idx), []).append("category missing")
        note("category_present", "Completeness", "Critical", rows, len(missing_category))

        cost_greater = df[pd.to_numeric(df["cost_price"], errors="coerce") > pd.to_numeric(df["mrp"], errors="coerce")].index
        failed_index.update(cost_greater)
        for idx in cost_greater:
            failure_map.setdefault(int(idx), []).append("cost_price exceeds mrp")
        note("cost_price_valid", "Accuracy", "Critical", rows, len(cost_greater))

        dup_product = df[df["product_id"].duplicated(keep=False)].index
        failed_index.update(dup_product)
        for idx in dup_product:
            failure_map.setdefault(int(idx), []).append("duplicate product_id")
        note("unique_product_id", "Uniqueness", "Critical", rows, len(dup_product))
        invalid_prices = df[(pd.to_numeric(df["mrp"], errors="coerce") <= 0) | pd.to_numeric(df["mrp"], errors="coerce").isna()].index
        failed_index.update(invalid_prices)
        for idx in invalid_prices:
            failure_map.setdefault(int(idx), []).append("mrp must be positive")
        note("positive_mrp", "Accuracy", "Critical", rows, len(invalid_prices))
        launch_dates = pd.to_datetime(df["launch_date"], errors="coerce")
        bad_launch_dates = df[launch_dates.isna() | (launch_dates > pd.Timestamp.now().normalize())].index
        failed_index.update(bad_launch_dates)
        for idx in bad_launch_dates:
            failure_map.setdefault(int(idx), []).append("launch_date invalid or in future")
        note("product_launch_date", "Timeliness", "Critical", rows, len(bad_launch_dates))

    elif type_name == "stores":
        rows = len(df)
        missing_zone = df[df["zone"].isna() | (df["zone"].astype(str).str.strip() == "")].index
        failed_index.update(missing_zone)
        for idx in missing_zone:
            failure_map.setdefault(int(idx), []).append("zone missing")
        note("zone_present", "Completeness", "Critical", rows, len(missing_zone))

        dup_store = df[df["store_id"].duplicated(keep=False)].index
        failed_index.update(dup_store)
        for idx in dup_store:
            failure_map.setdefault(int(idx), []).append("duplicate store_id")
        note("unique_store_id", "Uniqueness", "Critical", rows, len(dup_store))
        bad_zones = df[~df["zone"].fillna("").astype(str).str.strip().str.title().isin({"North", "South", "East", "West"})].index
        failed_index.update(bad_zones)
        for idx in bad_zones:
            failure_map.setdefault(int(idx), []).append("zone is not a supported zone")
        note("valid_zone", "Validity", "Critical", rows, len(bad_zones))
        bad_capacity = df[pd.to_numeric(df["capacity_orders_per_day"], errors="coerce").fillna(0) <= 0].index
        for idx in bad_capacity:
            failure_map.setdefault(int(idx), []).append("capacity_orders_per_day must be positive")
        note("positive_store_capacity", "Accuracy", "Warning", rows, len(bad_capacity))
        open_dates = pd.to_datetime(df["open_date"], errors="coerce")
        bad_open_dates = df[open_dates.isna() | (open_dates > pd.Timestamp.now().normalize())].index
        failed_index.update(bad_open_dates)
        for idx in bad_open_dates:
            failure_map.setdefault(int(idx), []).append("open_date invalid or in future")
        note("store_open_date", "Timeliness", "Critical", rows, len(bad_open_dates))

    passed = df.loc[~df.index.isin(failed_index)].copy()
    failed = df.loc[df.index.isin(failed_index)].copy()
    failed["failure_reason"] = failed.index.map(lambda idx: "; ".join(failure_map.get(int(idx), ["validation_failed"])))
    return passed, failed, dq_results
