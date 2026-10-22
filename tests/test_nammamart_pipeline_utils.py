import duckdb
import pandas as pd

from include.nammamart_pipeline_utils import (
    update_customer_loyalty_history,
    validate_file,
    write_dq_trend_chart,
)


def test_iso_order_timestamp_is_parsed_without_day_first_reinterpretation():
    order_ts = (pd.Timestamp.now().floor("s") - pd.Timedelta(hours=1)).isoformat()
    orders = pd.DataFrame(
        [
            {
                "order_id": "ORD-TEST-1",
                "order_ts": order_ts,
                "customer_id": "CUS-TEST-1",
                "store_id": "STORE-TEST-1",
                "product_id": "PROD-TEST-1",
                "quantity": 1,
                "unit_price": 10,
                "discount_pct": 0,
                "order_amount": 10,
                "payment_mode": "UPI",
                "order_status": "DELIVERED",
                "channel": "App",
                "delivery_minutes": 10,
            }
        ]
    )

    passed, failed, _ = validate_file(orders, "orders", "test", "test-run")

    assert len(passed) == 1
    assert failed.empty


def test_iso_payment_timestamp_is_accepted():
    payment_ts = (pd.Timestamp.now().floor("s") - pd.Timedelta(hours=1)).isoformat()
    payments = pd.DataFrame(
        [
            {
                "payment_id": "PAY-TEST-1",
                "order_id": "ORD-TEST-1",
                "payment_ts": payment_ts,
                "payment_mode": "UPI",
                "amount_paid": 10,
                "payment_status": "SUCCESS",
                "gateway_ref": "GATEWAY-TEST-1",
            }
        ]
    )

    passed, failed, _ = validate_file(payments, "payments", "test", "test-run")

    assert len(passed) == 1
    assert failed.empty


def test_customer_loyalty_history_closes_old_tier_and_ignores_unchanged_tier():
    with duckdb.connect(":memory:") as con:
        update_customer_loyalty_history(
            con,
            pd.DataFrame([{"customer_id": "CUS-1", "loyalty_tier": "Silver"}]),
        )
        update_customer_loyalty_history(
            con,
            pd.DataFrame([{"customer_id": "CUS-1", "loyalty_tier": "Silver"}]),
        )
        update_customer_loyalty_history(
            con,
            pd.DataFrame([{"customer_id": "CUS-1", "loyalty_tier": "Gold"}]),
        )

        history = con.execute(
            """
            SELECT loyalty_tier, valid_to IS NOT NULL AS closed, is_current
            FROM etl_dim_customer_loyalty_scd2
            ORDER BY valid_from
            """
        ).fetchall()

    assert history == [("Silver", True, False), ("Gold", False, True)]


def test_dq_trend_chart_uses_dq_results_rows(tmp_path):
    with duckdb.connect(":memory:") as con:
        con.execute(
            """
            CREATE TABLE dq_results (
                run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR,
                dimension VARCHAR, severity VARCHAR, rows_checked BIGINT,
                rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            INSERT INTO dq_results VALUES
                ('run-1', 'nammamart_etl_prasad', 'valid', 'Accuracy',
                 'Warning', 100, 5, 'warn', current_timestamp)
            """
        )
        chart = write_dq_trend_chart(con, tmp_path / "dq_trend.svg")
        contents = chart.read_text(encoding="utf-8")

    assert "<svg" in contents
    assert "Data-quality failure trend" in contents
    assert "nammamart_etl_prasad" in contents
