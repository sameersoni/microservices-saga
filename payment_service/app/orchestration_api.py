"""REST handlers used by saga orchestrator (no Kafka)."""

from __future__ import annotations

import logging
import random
from uuid import uuid4

from saga_common.events import (
    BaseEvent,
    EventType,
    PaymentFailedPayload,
    PaymentProcessedPayload,
    PaymentRefundedPayload,
)

from app.failure_config import get_failure_rate
from app.db import PaymentStatus, get_by_order, insert_payment, update_payment_status

log = logging.getLogger(__name__)


def charge_order(
    conn,
    *,
    order_id: str,
    correlation_id: str,
    amount_cents: int,
) -> BaseEvent:
    existing = get_by_order(conn, order_id)
    if existing:
        if existing.status == PaymentStatus.COMPLETED:
            return BaseEvent(
                correlation_id=correlation_id,
                event_type=EventType.PAYMENT_PROCESSED,
                payload=PaymentProcessedPayload(
                    order_id=order_id,
                    payment_id=existing.id,
                    amount_cents=amount_cents,
                ).model_dump(),
            )
        if existing.status == PaymentStatus.FAILED:
            return BaseEvent(
                correlation_id=correlation_id,
                event_type=EventType.PAYMENT_FAILED,
                payload=PaymentFailedPayload(
                    order_id=order_id,
                    reason="previous_failure",
                ).model_dump(),
            )

    payment_id = str(uuid4())
    fail = random.random() < get_failure_rate()
    if fail:
        insert_payment(
            conn,
            payment_id=payment_id,
            order_id=order_id,
            correlation_id=correlation_id,
            amount_cents=amount_cents,
            status=PaymentStatus.FAILED,
        )
        log.warning("Orchestration PAYMENT_FAILED (sim) order=%s", order_id)
        return BaseEvent(
            correlation_id=correlation_id,
            event_type=EventType.PAYMENT_FAILED,
            payload=PaymentFailedPayload(
                order_id=order_id,
                reason="simulated_payment_failure",
            ).model_dump(),
        )

    insert_payment(
        conn,
        payment_id=payment_id,
        order_id=order_id,
        correlation_id=correlation_id,
        amount_cents=amount_cents,
        status=PaymentStatus.COMPLETED,
    )
    return BaseEvent(
        correlation_id=correlation_id,
        event_type=EventType.PAYMENT_PROCESSED,
        payload=PaymentProcessedPayload(
            order_id=order_id,
            payment_id=payment_id,
            amount_cents=amount_cents,
        ).model_dump(),
    )


def refund_order(conn, *, order_id: str, correlation_id: str, reason: str) -> BaseEvent | None:
    row = get_by_order(conn, order_id)
    if not row:
        return None
    if row.status == PaymentStatus.REFUNDED:
        return None
    if row.status != PaymentStatus.COMPLETED:
        return None
    update_payment_status(conn, row.id, PaymentStatus.REFUNDED)
    return BaseEvent(
        correlation_id=correlation_id,
        event_type=EventType.PAYMENT_REFUNDED,
        payload=PaymentRefundedPayload(
            order_id=order_id,
            payment_id=row.id,
            reason=reason,
        ).model_dump(),
    )
