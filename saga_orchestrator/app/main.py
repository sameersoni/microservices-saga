"""Saga orchestrator — central REST-driven saga (orchestration pattern)."""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from saga_common.logging_conf import correlation_id_var, setup_logging
from saga_common.trace import new_trace_id

from app.config import DATABASE_PATH, LOG_LEVEL, PAYMENT_URL, SERVICE_NAME, INVENTORY_URL
from app.db import get_conn, get_saga, insert_saga, list_recent_sagas, list_saga_logs
from app.engine import run_orchestrated_saga_sync

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(SERVICE_NAME, LOG_LEVEL)
    yield


app = FastAPI(title="Saga Orchestrator", version="0.1.0", lifespan=lifespan)


class CreateSagaBody(BaseModel):
    customer_id: str = "demo"
    items: list[dict[str, Any]] = Field(default_factory=lambda: [{"sku": "SKU-DEMO", "qty": 1}])
    amount_cents: int = 1999


@app.post("/orchestration/sagas", summary="Start orchestrated saga (REST calls each service)")
async def start_saga(body: CreateSagaBody):
    saga_id = str(uuid4())
    correlation_id_var.set(saga_id)
    trace_id = new_trace_id()
    with get_conn(DATABASE_PATH) as conn:
        insert_saga(conn, saga_id, {"trace_id": trace_id, "customer_id": body.customer_id})

    try:
        result = await asyncio.to_thread(
            run_orchestrated_saga_sync,
            saga_id=saga_id,
            customer_id=body.customer_id,
            items=body.items,
            amount_cents=body.amount_cents,
        )
        result["trace_id"] = trace_id
        result["correlation_id"] = saga_id
        return result
    except Exception as e:  # noqa: BLE001
        log.exception("Orchestration failed: %s", e)
        with get_conn(DATABASE_PATH) as conn:
            from app.db import SagaState, append_log, update_saga

            update_saga(conn, saga_id, state=SagaState.FAILED, detail={"error": str(e)})
            append_log(conn, saga_id, f"ERROR: {e}\n{traceback.format_exc()}")
        raise HTTPException(500, detail=str(e)) from e


@app.get("/orchestration/sagas/{saga_id}")
async def get_saga_status(saga_id: str):
    def read():
        with get_conn(DATABASE_PATH) as conn:
            return get_saga(conn, saga_id), list_saga_logs(conn, saga_id)

    row, logs = await asyncio.to_thread(read)
    if not row:
        raise HTTPException(404)
    return {
        "saga_id": row.saga_id,
        "order_id": row.order_id,
        "state": row.state.value,
        "detail": json.loads(row.detail_json),
        "event_log": logs,
    }


@app.get("/orchestration/sagas")
async def list_sagas(limit: int = 30):
    def read():
        with get_conn(DATABASE_PATH) as conn:
            return list_recent_sagas(conn, limit)

    rows = await asyncio.to_thread(read)
    return [
        {
            "saga_id": r.saga_id,
            "order_id": r.order_id,
            "state": r.state.value,
            "updated_at": r.updated_at,
        }
        for r in rows
    ]


class SimulateBody(BaseModel):
    payment_failure_rate: float | None = None
    inventory_failure_rate: float | None = None


@app.post("/simulate/failures", summary="Tune simulated failure rates on payment & inventory")
async def simulate_failures(body: SimulateBody):
    import httpx

    out: dict[str, Any] = {}
    async with httpx.AsyncClient(timeout=15.0) as client:
        if body.payment_failure_rate is not None:
            r = await client.post(
                f"{PAYMENT_URL}/admin/failure-rate",
                json={"rate": body.payment_failure_rate},
            )
            r.raise_for_status()
            out["payment"] = r.json()
        if body.inventory_failure_rate is not None:
            r = await client.post(
                f"{INVENTORY_URL}/admin/failure-rate",
                json={"rate": body.inventory_failure_rate},
            )
            r.raise_for_status()
            out["inventory"] = r.json()
    return out


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/event-log", response_class=HTMLResponse, summary="Minimal HTML viewer for orchestrator saga logs")
async def event_log_view():
    def build_html() -> str:
        with get_conn(DATABASE_PATH) as conn:
            rows = list_recent_sagas(conn, 30)
            parts: list[str] = [
                "<html><head><title>Saga event log</title></head><body><h1>Recent orchestration sagas</h1>"
            ]
            for r in rows:
                logs = list_saga_logs(conn, r.saga_id)
                block = f"<h3>{r.saga_id} — {r.state.value}</h3><pre>{chr(10).join(logs)}</pre>"
                parts.append(block)
            parts.append("</body></html>")
            return "".join(parts)

    return HTMLResponse(await asyncio.to_thread(build_html))
