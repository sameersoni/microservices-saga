from __future__ import annotations

from saga_common.events import BaseEvent, EventType, InventoryFailedPayload, OrderCreatedPayload

from conftest import load_service_module


def test_payment_success_then_refund_compensation(tmp_path):
    db = load_service_module("payment_service", "app.db")
    charge = load_service_module("payment_service", "app.charge")

    db_path = tmp_path / "payment.db"
    with db.get_conn(str(db_path)) as conn:
        charge.get_failure_rate = lambda: 0.0
        created = BaseEvent(
            event_id="ev-order-1",
            correlation_id="corr-1",
            event_type=EventType.ORDER_CREATED,
            payload=OrderCreatedPayload(
                order_id="order-1",
                amount_cents=2500,
                items=[{"sku": "SKU-DEMO", "qty": 1}],
            ).model_dump(),
        )

        out = charge.process_event(conn, created)
        assert len(out) == 1
        assert out[0].event_type == EventType.PAYMENT_PROCESSED
        assert db.get_by_order(conn, "order-1").status == db.PaymentStatus.COMPLETED

        inv_failed = BaseEvent(
            event_id="ev-inv-failed-1",
            correlation_id="corr-1",
            event_type=EventType.INVENTORY_FAILED,
            payload=InventoryFailedPayload(
                order_id="order-1",
                sku="SKU-DEMO",
                reason="insufficient_stock",
            ).model_dump(),
        )
        out2 = charge.process_event(conn, inv_failed)
        assert len(out2) == 1
        assert out2[0].event_type == EventType.PAYMENT_REFUNDED
        assert db.get_by_order(conn, "order-1").status == db.PaymentStatus.REFUNDED

        # Replaying failure after refund should be idempotent.
        inv_failed_again = BaseEvent(
            event_id="ev-inv-failed-2",
            correlation_id="corr-1",
            event_type=EventType.INVENTORY_FAILED,
            payload=InventoryFailedPayload(
                order_id="order-1",
                sku="SKU-DEMO",
                reason="insufficient_stock",
            ).model_dump(),
        )
        out3 = charge.process_event(conn, inv_failed_again)
        assert out3 == []


def test_payment_failure_event_when_failure_rate_is_one(tmp_path):
    db = load_service_module("payment_service", "app.db")
    charge = load_service_module("payment_service", "app.charge")

    db_path = tmp_path / "payment.db"
    with db.get_conn(str(db_path)) as conn:
        charge.get_failure_rate = lambda: 1.0
        created = BaseEvent(
            event_id="ev-order-fail",
            correlation_id="corr-fail",
            event_type=EventType.ORDER_CREATED,
            payload=OrderCreatedPayload(
                order_id="order-fail",
                amount_cents=500,
                items=[{"sku": "SKU-DEMO", "qty": 1}],
            ).model_dump(),
        )
        out = charge.process_event(conn, created)
        assert len(out) == 1
        assert out[0].event_type == EventType.PAYMENT_FAILED
        assert db.get_by_order(conn, "order-fail").status == db.PaymentStatus.FAILED
