"""Choreography handlers for inventory."""

from __future__ import annotations

import logging
import random
from uuid import uuid4

from saga_common.events import (
    BaseEvent,
    EventType,
    InventoryFailedPayload,
    InventoryReservedPayload,
    PaymentProcessedPayload,
)

from app.config import INITIAL_STOCK
from app.failure_config import get_failure_rate
from app.db import ReservationStatus, get_reservation_by_order, insert_reservation, try_decrement_stock, try_record_event

log = logging.getLogger(__name__)

DEFAULT_SKU = "SKU-DEMO"


def process_event(conn, ev: BaseEvent) -> list[BaseEvent]:
    out: list[BaseEvent] = []
    if not try_record_event(conn, ev.event_id, ev.correlation_id):
        log.info("Duplicate event skipped: %s", ev.event_id)
        return out

    if ev.event_type != EventType.PAYMENT_PROCESSED:
        log.debug("Inventory ignoring %s", ev.event_type)
        return out

    payload = PaymentProcessedPayload.model_validate(ev.payload)
    if get_reservation_by_order(conn, payload.order_id):
        log.info("Reservation already exists for %s", payload.order_id)
        return out

    qty = 1
    sku = DEFAULT_SKU
    reservation_id = str(uuid4())

    if random.random() < get_failure_rate():
        insert_reservation(
            conn,
            reservation_id=reservation_id,
            order_id=payload.order_id,
            correlation_id=ev.correlation_id,
            sku=sku,
            quantity=qty,
            status=ReservationStatus.FAILED,
        )
        out.append(
            BaseEvent(
                correlation_id=ev.correlation_id,
                event_type=EventType.INVENTORY_FAILED,
                payload=InventoryFailedPayload(
                    order_id=payload.order_id,
                    sku=sku,
                    reason="simulated_inventory_failure",
                ).model_dump(),
            )
        )
        log.warning("INVENTORY_FAILED (simulated) for order %s", payload.order_id)
        return out

    if not try_decrement_stock(conn, sku, qty):
        insert_reservation(
            conn,
            reservation_id=reservation_id,
            order_id=payload.order_id,
            correlation_id=ev.correlation_id,
            sku=sku,
            quantity=qty,
            status=ReservationStatus.FAILED,
        )
        out.append(
            BaseEvent(
                correlation_id=ev.correlation_id,
                event_type=EventType.INVENTORY_FAILED,
                payload=InventoryFailedPayload(
                    order_id=payload.order_id,
                    sku=sku,
                    reason="insufficient_stock",
                ).model_dump(),
            )
        )
        log.warning("INVENTORY_FAILED (stock) for order %s", payload.order_id)
        return out

    insert_reservation(
        conn,
        reservation_id=reservation_id,
        order_id=payload.order_id,
        correlation_id=ev.correlation_id,
        sku=sku,
        quantity=qty,
        status=ReservationStatus.RESERVED,
    )
    out.append(
        BaseEvent(
            correlation_id=ev.correlation_id,
            event_type=EventType.INVENTORY_RESERVED,
            payload=InventoryReservedPayload(
                order_id=payload.order_id,
                reservation_id=reservation_id,
                sku=sku,
                quantity=qty,
            ).model_dump(),
        )
    )
    log.info("INVENTORY_RESERVED for order %s", payload.order_id)
    return out
