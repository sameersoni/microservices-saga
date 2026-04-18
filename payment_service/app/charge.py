"""Choreography handlers for payment service."""

from __future__ import annotations

import logging
import random
from uuid import uuid4

from saga_common.events import (
    BaseEvent,
    EventType,
    InventoryFailedPayload,
    OrderCreatedPayload,
    PaymentFailedPayload,
    PaymentProcessedPayload,
    PaymentRefundedPayload,
)

from app.failure_config import get_failure_rate
from app.db import PaymentStatus, get_by_order, insert_payment, try_record_event, update_payment_status

log = logging.getLogger(__name__)


def process_event(conn, ev: BaseEvent) -> list[BaseEvent]:
    out: list[BaseEvent] = []
    if not try_record_event(conn, ev.event_id, ev.correlation_id):
        log.info("Duplicate event skipped: %s", ev.event_id)
        return out

    if ev.event_type == EventType.ORDER_CREATED:
        out.extend(_on_order_created(conn, ev))
    elif ev.event_type == EventType.INVENTORY_FAILED:
        out.extend(_on_inventory_failed(conn, ev))
    else:
        log.debug("Payment service ignoring %s", ev.event_type)
    return out


def _on_order_created(conn, ev: BaseEvent) -> list[BaseEvent]:
    out: list[BaseEvent] = []
    payload = OrderCreatedPayload.model_validate(ev.payload)
    if get_by_order(conn, payload.order_id):
        log.info("Payment already exists for order %s", payload.order_id)
        return out

    payment_id = str(uuid4())
    fail = random.random() < get_failure_rate()
    if fail:
        insert_payment(
            conn,
            payment_id=payment_id,
            order_id=payload.order_id,
            correlation_id=ev.correlation_id,
            amount_cents=payload.amount_cents,
            status=PaymentStatus.FAILED,
        )
        out.append(
            BaseEvent(
                correlation_id=ev.correlation_id,
                event_type=EventType.PAYMENT_FAILED,
                payload=PaymentFailedPayload(
                    order_id=payload.order_id,
                    reason="simulated_payment_failure",
                ).model_dump(),
            )
        )
        log.warning("PAYMENT_FAILED for order %s (simulated)", payload.order_id)
        return out

    insert_payment(
        conn,
        payment_id=payment_id,
        order_id=payload.order_id,
        correlation_id=ev.correlation_id,
        amount_cents=payload.amount_cents,
        status=PaymentStatus.COMPLETED,
    )
    out.append(
        BaseEvent(
            correlation_id=ev.correlation_id,
            event_type=EventType.PAYMENT_PROCESSED,
            payload=PaymentProcessedPayload(
                order_id=payload.order_id,
                payment_id=payment_id,
                amount_cents=payload.amount_cents,
            ).model_dump(),
        )
    )
    log.info("PAYMENT_PROCESSED for order %s", payload.order_id)
    return out


def _on_inventory_failed(conn, ev: BaseEvent) -> list[BaseEvent]:
    out: list[BaseEvent] = []
    payload = InventoryFailedPayload.model_validate(ev.payload)
    row = get_by_order(conn, payload.order_id)
    if not row or row.status != PaymentStatus.COMPLETED:
        log.warning("Cannot refund: payment state for order %s", payload.order_id)
        return out

    update_payment_status(conn, row.id, PaymentStatus.REFUNDED)
    out.append(
        BaseEvent(
            correlation_id=ev.correlation_id,
            event_type=EventType.PAYMENT_REFUNDED,
            payload=PaymentRefundedPayload(
                order_id=payload.order_id,
                payment_id=row.id,
                reason=f"inventory_failed:{payload.reason}",
            ).model_dump(),
        )
    )
    log.info("PAYMENT_REFUNDED for order %s", payload.order_id)
    return out
