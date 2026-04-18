from __future__ import annotations

from saga_common.events import (
    BaseEvent,
    EventType,
    InventoryReservedPayload,
    PaymentProcessedPayload,
)

from conftest import load_service_module


def test_order_transitions_to_completed_and_emits_event(tmp_path):
    db = load_service_module("order_service", "app.db")
    handlers = load_service_module("order_service", "app.events_handling")

    db_path = tmp_path / "order.db"
    with db.get_conn(str(db_path)) as conn:
        db.insert_order(
            conn,
            order_id="order-1",
            correlation_id="corr-1",
            amount_cents=1000,
            items=[{"sku": "SKU-DEMO", "qty": 1}],
        )

        payment_ev = BaseEvent(
            event_id="ev-payment-1",
            correlation_id="corr-1",
            event_type=EventType.PAYMENT_PROCESSED,
            payload=PaymentProcessedPayload(
                order_id="order-1",
                payment_id="pay-1",
                amount_cents=1000,
            ).model_dump(),
        )
        out1 = handlers.process_order_event(conn, payment_ev)
        assert out1 == []
        assert db.get_by_id(conn, "order-1").status == db.OrderStatus.PAYMENT_COMPLETED

        inv_ev = BaseEvent(
            event_id="ev-inv-1",
            correlation_id="corr-1",
            event_type=EventType.INVENTORY_RESERVED,
            payload=InventoryReservedPayload(
                order_id="order-1",
                reservation_id="res-1",
                sku="SKU-DEMO",
                quantity=1,
            ).model_dump(),
        )
        out2 = handlers.process_order_event(conn, inv_ev)
        assert len(out2) == 1
        assert out2[0].event_type == EventType.ORDER_COMPLETED
        assert db.get_by_id(conn, "order-1").status == db.OrderStatus.COMPLETED


def test_order_event_idempotency_skips_duplicate_event(tmp_path):
    db = load_service_module("order_service", "app.db")
    handlers = load_service_module("order_service", "app.events_handling")

    db_path = tmp_path / "order.db"
    with db.get_conn(str(db_path)) as conn:
        db.insert_order(
            conn,
            order_id="order-2",
            correlation_id="corr-2",
            amount_cents=999,
            items=[{"sku": "SKU-DEMO", "qty": 1}],
        )
        db.update_status(conn, "order-2", db.OrderStatus.PAYMENT_COMPLETED)

        inv_ev = BaseEvent(
            event_id="ev-inv-duplicate",
            correlation_id="corr-2",
            event_type=EventType.INVENTORY_RESERVED,
            payload=InventoryReservedPayload(
                order_id="order-2",
                reservation_id="res-2",
                sku="SKU-DEMO",
                quantity=1,
            ).model_dump(),
        )

        first = handlers.process_order_event(conn, inv_ev)
        second = handlers.process_order_event(conn, inv_ev)

        assert len(first) == 1
        assert second == []
