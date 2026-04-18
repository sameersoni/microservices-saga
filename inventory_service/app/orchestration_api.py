"""REST reserve path for orchestrator."""

from __future__ import annotations

import logging
import random
from uuid import uuid4

from saga_common.events import (
    BaseEvent,
    EventType,
    InventoryFailedPayload,
    InventoryReservedPayload,
)

from app.db import ReservationStatus, get_reservation_by_order, insert_reservation, try_decrement_stock
from app.failure_config import get_failure_rate
from app.reserve import DEFAULT_SKU

log = logging.getLogger(__name__)


def reserve_for_order(
    conn,
    *,
    order_id: str,
    correlation_id: str,
    sku: str = DEFAULT_SKU,
    quantity: int = 1,
) -> BaseEvent:
    if get_reservation_by_order(conn, order_id):
        row = get_reservation_by_order(conn, order_id)
        assert row is not None
        if row.status == ReservationStatus.RESERVED:
            return BaseEvent(
                correlation_id=correlation_id,
                event_type=EventType.INVENTORY_RESERVED,
                payload=InventoryReservedPayload(
                    order_id=order_id,
                    reservation_id=row.id,
                    sku=row.sku,
                    quantity=row.quantity,
                ).model_dump(),
            )
        return BaseEvent(
            correlation_id=correlation_id,
            event_type=EventType.INVENTORY_FAILED,
            payload=InventoryFailedPayload(
                order_id=order_id,
                sku=sku,
                reason="previous_failure",
            ).model_dump(),
        )

    reservation_id = str(uuid4())

    if random.random() < get_failure_rate():
        insert_reservation(
            conn,
            reservation_id=reservation_id,
            order_id=order_id,
            correlation_id=correlation_id,
            sku=sku,
            quantity=quantity,
            status=ReservationStatus.FAILED,
        )
        return BaseEvent(
            correlation_id=correlation_id,
            event_type=EventType.INVENTORY_FAILED,
            payload=InventoryFailedPayload(
                order_id=order_id,
                sku=sku,
                reason="simulated_inventory_failure",
            ).model_dump(),
        )

    if not try_decrement_stock(conn, sku, quantity):
        insert_reservation(
            conn,
            reservation_id=reservation_id,
            order_id=order_id,
            correlation_id=correlation_id,
            sku=sku,
            quantity=quantity,
            status=ReservationStatus.FAILED,
        )
        return BaseEvent(
            correlation_id=correlation_id,
            event_type=EventType.INVENTORY_FAILED,
            payload=InventoryFailedPayload(
                order_id=order_id,
                sku=sku,
                reason="insufficient_stock",
            ).model_dump(),
        )

    insert_reservation(
        conn,
        reservation_id=reservation_id,
        order_id=order_id,
        correlation_id=correlation_id,
        sku=sku,
        quantity=quantity,
        status=ReservationStatus.RESERVED,
    )
    log.info("Orchestration INVENTORY_RESERVED for %s", order_id)
    return BaseEvent(
        correlation_id=correlation_id,
        event_type=EventType.INVENTORY_RESERVED,
        payload=InventoryReservedPayload(
            order_id=order_id,
            reservation_id=reservation_id,
            sku=sku,
            quantity=quantity,
        ).model_dump(),
    )
