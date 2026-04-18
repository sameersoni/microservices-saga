"""Order service — REST + choreography producer + Kafka consumer."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from saga_common.events import BaseEvent, EventType, OrderCreatedPayload
from saga_common.kafka_bus import TOPIC_SAGA, create_producer, publish_event
from saga_common.kafka_worker import run_saga_consumer
from saga_common.logging_conf import correlation_id_var, setup_logging
from saga_common.retry import retry_async

from app.config import DATABASE_PATH, KAFKA_BOOTSTRAP, LOG_LEVEL, SERVICE_NAME
from app.db import OrderStatus, get_by_correlation, get_by_id, get_conn, insert_order, list_recent_orders, update_status
from app.kafka_handlers import saga_handler

log = logging.getLogger(__name__)

producer_holder: dict[str, Any] = {}
consumer_stop: asyncio.Event | None = None
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_stop, consumer_task
    setup_logging(SERVICE_NAME, LOG_LEVEL)
    consumer_stop = asyncio.Event()
    async def start_prod():
        return await create_producer(KAFKA_BOOTSTRAP)

    try:
        prod = await retry_async(
            start_prod,
            max_attempts=15,
            base_delay_s=0.5,
            operation="kafka_producer_start",
        )
    except (KafkaConnectionError, OSError) as e:
        log.error("Kafka unavailable: %s — choreography disabled until broker is up.", e)
        prod = None
    producer_holder["prod"] = prod
    if prod:
        consumer_task = asyncio.create_task(
            run_saga_consumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="order-service",
                producer=prod,
                handler=saga_handler,
                stop=consumer_stop,
            )
        )
    yield
    if consumer_stop:
        consumer_stop.set()
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    p = producer_holder.get("prod")
    if p:
        await p.stop()


app = FastAPI(title="Order Service", lifespan=lifespan)


class CreateOrderBody(BaseModel):
    customer_id: str = "demo-customer"
    items: list[dict[str, Any]] = Field(default_factory=lambda: [{"sku": "SKU-DEMO", "qty": 1}])
    amount_cents: int = 1999


@app.post("/orders", summary="Create order (choreography publishes ORDER_CREATED unless orchestrated)")
async def create_order(
    body: CreateOrderBody,
    x_orchestrated: str | None = Header(None, alias="X-Orchestrated"),
):
    correlation_id = str(uuid4())
    order_id = str(uuid4())
    correlation_id_var.set(correlation_id)
    orchestrated = (x_orchestrated or "").lower() in ("1", "true", "yes")

    def work() -> None:
        with get_conn(DATABASE_PATH) as conn:
            insert_order(
                conn,
                order_id=order_id,
                correlation_id=correlation_id,
                amount_cents=body.amount_cents,
                items=body.items,
            )

    await asyncio.to_thread(work)

    if not orchestrated:
        prod = producer_holder.get("prod")
        if not prod:
            raise HTTPException(503, "Kafka producer not available; cannot start choreography saga")
        ev = BaseEvent(
            correlation_id=correlation_id,
            event_type=EventType.ORDER_CREATED,
            payload=OrderCreatedPayload(
                order_id=order_id,
                amount_cents=body.amount_cents,
                items=body.items,
            ).model_dump(),
        )
        await publish_event(prod, TOPIC_SAGA, ev)

    log.info(
        "Order created order_id=%s correlation_id=%s orchestrated=%s",
        order_id,
        correlation_id,
        orchestrated,
    )
    return {
        "order_id": order_id,
        "correlation_id": correlation_id,
        "mode": "orchestrated" if orchestrated else "choreography",
    }


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    def read():
        with get_conn(DATABASE_PATH) as conn:
            return get_by_id(conn, order_id)

    row = await asyncio.to_thread(read)
    if not row:
        raise HTTPException(404)
    return {
        "order_id": row.id,
        "correlation_id": row.correlation_id,
        "status": row.status.value,
        "amount_cents": row.amount_cents,
        "items": json.loads(row.items_json),
        "updated_at": row.updated_at,
    }


@app.get("/orders/by-correlation/{correlation_id}")
async def by_corr(correlation_id: str):
    def read():
        with get_conn(DATABASE_PATH) as conn:
            return get_by_correlation(conn, correlation_id)

    row = await asyncio.to_thread(read)
    if not row:
        raise HTTPException(404)
    return {
        "order_id": row.id,
        "correlation_id": row.correlation_id,
        "status": row.status.value,
        "amount_cents": row.amount_cents,
    }


@app.get("/orders")
async def list_orders(limit: int = 50):
    def read():
        with get_conn(DATABASE_PATH) as conn:
            return list_recent_orders(conn, limit)

    rows = await asyncio.to_thread(read)
    return [
        {
            "order_id": r.id,
            "correlation_id": r.correlation_id,
            "status": r.status.value,
            "amount_cents": r.amount_cents,
            "updated_at": r.updated_at,
        }
        for r in rows
    ]


class StatusPatchBody(BaseModel):
    status: OrderStatus


@app.patch("/orders/{order_id}/status", summary="Internal: orchestrator updates order status")
async def patch_order_status(
    order_id: str,
    body: StatusPatchBody,
    x_orchestrated: str | None = Header(None, alias="X-Orchestrated"),
):
    if (x_orchestrated or "").lower() not in ("1", "true", "yes"):
        raise HTTPException(403, "Only saga orchestrator may patch status")

    def work():
        with get_conn(DATABASE_PATH) as conn:
            row = get_by_id(conn, order_id)
            if not row:
                raise KeyError("missing")
            update_status(conn, order_id, body.status)

    try:
        await asyncio.to_thread(work)
    except KeyError:
        raise HTTPException(404)
    log.info("Order %s status patched -> %s", order_id, body.status.value)
    return {"order_id": order_id, "status": body.status.value}


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}
