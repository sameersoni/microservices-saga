"""Payment service — choreography consumer + REST for orchestration."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from saga_common.kafka_bus import create_producer
from saga_common.kafka_worker import run_saga_consumer
from saga_common.logging_conf import setup_logging
from saga_common.retry import retry_async

from app.config import DATABASE_PATH, KAFKA_BOOTSTRAP, LOG_LEVEL, SERVICE_NAME
from app.db import get_conn
from app.failure_config import get_failure_rate, set_failure_rate
from app.kafka_handlers import saga_handler
from app.orchestration_api import charge_order, refund_order

log = logging.getLogger(__name__)

producer_holder: dict = {}
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
        log.error("Kafka unavailable: %s", e)
        prod = None
    producer_holder["prod"] = prod
    if prod:
        consumer_task = asyncio.create_task(
            run_saga_consumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="payment-service",
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


app = FastAPI(title="Payment Service", lifespan=lifespan)


class ChargeBody(BaseModel):
    order_id: str
    correlation_id: str
    amount_cents: int


@app.post("/payments/charge", summary="Orchestration: charge order")
async def charge(body: ChargeBody):
    def work():
        with get_conn(DATABASE_PATH) as conn:
            return charge_order(
                conn,
                order_id=body.order_id,
                correlation_id=body.correlation_id,
                amount_cents=body.amount_cents,
            )

    ev = await asyncio.to_thread(work)
    return {"event_type": ev.event_type.value, "payload": dict(ev.payload)}


class RefundBody(BaseModel):
    order_id: str
    correlation_id: str
    reason: str = "compensation"


@app.post("/payments/refund", summary="Orchestration: refund payment")
async def refund(body: RefundBody):
    def work():
        with get_conn(DATABASE_PATH) as conn:
            return refund_order(
                conn,
                order_id=body.order_id,
                correlation_id=body.correlation_id,
                reason=body.reason,
            )

    ev = await asyncio.to_thread(work)
    if not ev:
        return {"refunded": False}
    return {"refunded": True, "event_type": ev.event_type.value}


class FailureBody(BaseModel):
    rate: float


@app.post("/admin/failure-rate")
async def set_rate(body: FailureBody):
    set_failure_rate(body.rate)
    log.warning("Payment failure rate set to %s", body.rate)
    return {"payment_failure_rate": get_failure_rate()}


@app.get("/payments/by-order/{order_id}")
async def get_pay(order_id: str):
    from app.db import get_by_order

    def read():
        with get_conn(DATABASE_PATH) as conn:
            return get_by_order(conn, order_id)

    row = await asyncio.to_thread(read)
    if not row:
        raise HTTPException(404)
    return {
        "payment_id": row.id,
        "order_id": row.order_id,
        "status": row.status.value,
        "amount_cents": row.amount_cents,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}
