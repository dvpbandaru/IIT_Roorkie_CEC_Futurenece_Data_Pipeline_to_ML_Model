import pandas as pd

from include.nammamart_pipeline_utils import validate_file


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
