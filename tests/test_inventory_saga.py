from __future__ import annotations

from saga_common.events import BaseEvent, EventType, PaymentProcessedPayload

from conftest import load_service_module


def _read_stock(conn, sku: str) -> int:
    row = conn.execute("SELECT quantity FROM stock WHERE sku = ?", (sku,)).fetchone()
    assert row is not None
    return int(row["quantity"])


def test_inventory_reserves_stock_and_emits_reserved(tmp_path):
    db = load_service_module("inventory_service", "app.db")
    reserve = load_service_module("inventory_service", "app.reserve")
    failure_config = load_service_module("inventory_service", "app.failure_config")

    db_path = tmp_path / "inventory.db"
    with db.get_conn(str(db_path)) as conn:
        db.ensure_stock(conn, reserve.DEFAULT_SKU, 3)
        failure_config.set_failure_rate(0.0)

        ev = BaseEvent(
            event_id="ev-pay-1",
            correlation_id="corr-1",
            event_type=EventType.PAYMENT_PROCESSED,
            payload=PaymentProcessedPayload(
                order_id="order-1",
                payment_id="pay-1",
                amount_cents=1000,
            ).model_dump(),
        )
        out = reserve.process_event(conn, ev)
        assert len(out) == 1
        assert out[0].event_type == EventType.INVENTORY_RESERVED
        assert _read_stock(conn, reserve.DEFAULT_SKU) == 2


def test_inventory_fails_when_stock_is_exhausted(tmp_path):
    db = load_service_module("inventory_service", "app.db")
    reserve = load_service_module("inventory_service", "app.reserve")
    failure_config = load_service_module("inventory_service", "app.failure_config")

    db_path = tmp_path / "inventory.db"
    with db.get_conn(str(db_path)) as conn:
        db.ensure_stock(conn, reserve.DEFAULT_SKU, 1)
        failure_config.set_failure_rate(0.0)

        first = BaseEvent(
            event_id="ev-pay-first",
            correlation_id="corr-1",
            event_type=EventType.PAYMENT_PROCESSED,
            payload=PaymentProcessedPayload(
                order_id="order-1",
                payment_id="pay-1",
                amount_cents=1000,
            ).model_dump(),
        )
        second = BaseEvent(
            event_id="ev-pay-second",
            correlation_id="corr-2",
            event_type=EventType.PAYMENT_PROCESSED,
            payload=PaymentProcessedPayload(
                order_id="order-2",
                payment_id="pay-2",
                amount_cents=1000,
            ).model_dump(),
        )

        out1 = reserve.process_event(conn, first)
        out2 = reserve.process_event(conn, second)

        assert out1[0].event_type == EventType.INVENTORY_RESERVED
        assert out2[0].event_type == EventType.INVENTORY_FAILED
        assert out2[0].payload["reason"] == "insufficient_stock"
