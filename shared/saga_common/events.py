"""Event schema definitions for choreography saga."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping
from uuid import uuid4

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EventType(str, Enum):
    ORDER_CREATED = "ORDER_CREATED"
    PAYMENT_PROCESSED = "PAYMENT_PROCESSED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    PAYMENT_REFUNDED = "PAYMENT_REFUNDED"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    INVENTORY_FAILED = "INVENTORY_FAILED"
    ORDER_COMPLETED = "ORDER_COMPLETED"
    ORDER_CANCELLED = "ORDER_CANCELLED"


class BaseEvent(BaseModel):
    """Envelope for all Kafka / internal events."""

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    correlation_id: str
    event_type: EventType
    occurred_at: datetime = Field(default_factory=utcnow)
    payload: Mapping[str, Any]

    model_config = {"populate_by_name": True}


class OrderCreatedPayload(BaseModel):
    order_id: str
    amount_cents: int
    items: list[dict[str, Any]]


class PaymentProcessedPayload(BaseModel):
    order_id: str
    payment_id: str
    amount_cents: int


class PaymentFailedPayload(BaseModel):
    order_id: str
    reason: str


class PaymentRefundedPayload(BaseModel):
    order_id: str
    payment_id: str
    reason: str


class InventoryReservedPayload(BaseModel):
    order_id: str
    reservation_id: str
    sku: str
    quantity: int


class InventoryFailedPayload(BaseModel):
    order_id: str
    sku: str
    reason: str


class OrderCompletedPayload(BaseModel):
    order_id: str


class OrderCancelledPayload(BaseModel):
    order_id: str
    reason: str


def parse_event(raw: bytes) -> BaseEvent:
    return BaseEvent.model_validate_json(raw.decode("utf-8"))
