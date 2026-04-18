"""Choreography: process incoming saga events for orders."""

from __future__ import annotations

import logging

from saga_common.events import (
    BaseEvent,
    EventType,
    InventoryReservedPayload,
    PaymentFailedPayload,
    PaymentProcessedPayload,
    PaymentRefundedPayload,
)

from app.db import OrderStatus, get_by_id, try_record_event, update_status

log = logging.getLogger(__name__)


def process_order_event(conn, ev: BaseEvent) -> list[BaseEvent]:
    """Returns follow-up events to publish (e.g. ORDER_COMPLETED)."""
    out: list[BaseEvent] = []
    if not try_record_event(conn, ev.event_id, ev.correlation_id):
        log.info("Duplicate event skipped: %s", ev.event_id)
        return out

    if ev.event_type == EventType.PAYMENT_PROCESSED:
        p = PaymentProcessedPayload.model_validate(ev.payload)
        _apply_payment_processed(conn, p)
    elif ev.event_type == EventType.PAYMENT_FAILED:
        p = PaymentFailedPayload.model_validate(ev.payload)
        _apply_payment_failed(conn, p)
    elif ev.event_type == EventType.INVENTORY_RESERVED:
        p = InventoryReservedPayload.model_validate(ev.payload)
        follow = _apply_inventory_reserved(conn, p, ev.correlation_id)
        if follow:
            out.append(follow)
    elif ev.event_type == EventType.PAYMENT_REFUNDED:
        p = PaymentRefundedPayload.model_validate(ev.payload)
        follow = _apply_payment_refunded(conn, p, ev.correlation_id)
        if follow:
            out.append(follow)
    else:
        log.debug("Order service ignoring event type %s", ev.event_type)
    return out


def _apply_payment_processed(conn, p: PaymentProcessedPayload) -> None:
    row = get_by_id(conn, p.order_id)
    if not row:
        log.warning("Order not found for PAYMENT_PROCESSED: %s", p.order_id)
        return
    if row.status == OrderStatus.CREATED:
        update_status(conn, p.order_id, OrderStatus.PAYMENT_COMPLETED)
        log.info("Order %s -> PAYMENT_COMPLETED", p.order_id)


def _apply_payment_failed(conn, p: PaymentFailedPayload) -> None:
    row = get_by_id(conn, p.order_id)
    if not row:
        return
    if row.status in (OrderStatus.CREATED, OrderStatus.PAYMENT_COMPLETED):
        update_status(conn, p.order_id, OrderStatus.FAILED)
        log.warning("Order %s -> FAILED (payment): %s", p.order_id, p.reason)


def _apply_inventory_reserved(conn, p: InventoryReservedPayload, correlation_id: str) -> BaseEvent | None:
    row = get_by_id(conn, p.order_id)
    if not row:
        return None
    if row.status == OrderStatus.COMPLETED:
        log.info("Order %s already completed; idempotent skip", p.order_id)
        return None
    if row.status == OrderStatus.PAYMENT_COMPLETED:
        update_status(conn, p.order_id, OrderStatus.INVENTORY_RESERVED)
    update_status(conn, p.order_id, OrderStatus.COMPLETED)
    log.info("Order %s -> COMPLETED", p.order_id)
    from saga_common.events import OrderCompletedPayload

    return BaseEvent(
        correlation_id=correlation_id,
        event_type=EventType.ORDER_COMPLETED,
        payload=OrderCompletedPayload(order_id=p.order_id).model_dump(),
    )


def _apply_payment_refunded(conn, p: PaymentRefundedPayload, correlation_id: str) -> BaseEvent | None:
    row = get_by_id(conn, p.order_id)
    if not row:
        return None
    if row.status == OrderStatus.CANCELLED:
        return None
    update_status(conn, p.order_id, OrderStatus.CANCELLED)
    log.info("Order %s -> CANCELLED (refund: %s)", p.order_id, p.reason)
    from saga_common.events import OrderCancelledPayload

    return BaseEvent(
        correlation_id=correlation_id,
        event_type=EventType.ORDER_CANCELLED,
        payload=OrderCancelledPayload(
            order_id=p.order_id,
            reason=p.reason,
        ).model_dump(),
    )
