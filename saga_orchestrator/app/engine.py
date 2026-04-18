"""Orchestration saga — REST sequence with compensation."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import DATABASE_PATH, INVENTORY_URL, ORDER_URL, PAYMENT_URL
from app.db import SagaState, append_log, get_conn, update_saga

log = logging.getLogger(__name__)

ORCH_HEADERS = {"X-Orchestrated": "true"}


def run_orchestrated_saga_sync(
    *,
    saga_id: str,
    customer_id: str,
    items: list[dict[str, Any]],
    amount_cents: int,
) -> dict[str, Any]:
    """Blocking saga (run in worker thread). Uses sync httpx."""
    with get_conn(DATABASE_PATH) as conn, httpx.Client(timeout=30.0) as client:

        def slog(msg: str) -> None:
            append_log(conn, saga_id, msg)
            log.info("[saga=%s] %s", saga_id, msg)

        slog("Saga started (orchestration)")

        r = client.post(
            f"{ORDER_URL}/orders",
            json={
                "customer_id": customer_id,
                "items": items,
                "amount_cents": amount_cents,
            },
            headers=ORCH_HEADERS,
        )
        r.raise_for_status()
        oj = r.json()
        order_id = oj["order_id"]
        correlation_id = oj["correlation_id"]
        update_saga(conn, saga_id, order_id=order_id, state=SagaState.ORDER_CREATED, detail={"correlation_id": correlation_id})
        slog(f"Order created order_id={order_id}")

        pr = client.post(
            f"{PAYMENT_URL}/payments/charge",
            json={
                "order_id": order_id,
                "correlation_id": correlation_id,
                "amount_cents": amount_cents,
            },
        )
        pr.raise_for_status()
        pj = pr.json()
        if pj["event_type"] == "PAYMENT_FAILED":
            _patch_order(client, order_id, "FAILED")
            update_saga(conn, saga_id, state=SagaState.FAILED, detail={"step": "payment"})
            slog("Payment failed — order marked FAILED")
            return {"saga_id": saga_id, "state": "FAILED", "order_id": order_id}

        _patch_order(client, order_id, "PAYMENT_COMPLETED")
        update_saga(conn, saga_id, state=SagaState.PAYMENT_COMPLETED)
        slog("Payment completed")

        ir = client.post(
            f"{INVENTORY_URL}/inventory/reserve",
            json={
                "order_id": order_id,
                "correlation_id": correlation_id,
                "sku": items[0].get("sku", "SKU-DEMO"),
                "quantity": int(items[0].get("qty", 1)),
            },
        )
        ir.raise_for_status()
        ij = ir.json()
        if ij["event_type"] == "INVENTORY_FAILED":
            slog("Inventory failed — compensating (refund)")
            update_saga(conn, saga_id, state=SagaState.COMPENSATING)
            refund = client.post(
                f"{PAYMENT_URL}/payments/refund",
                json={
                    "order_id": order_id,
                    "correlation_id": correlation_id,
                    "reason": "inventory_failed",
                },
            )
            refund.raise_for_status()
            _patch_order(client, order_id, "CANCELLED")
            update_saga(conn, saga_id, state=SagaState.CANCELLED, detail={"compensated": True})
            slog("Refund issued; order CANCELLED")
            return {"saga_id": saga_id, "state": "CANCELLED", "order_id": order_id}

        _patch_order(client, order_id, "INVENTORY_RESERVED")
        update_saga(conn, saga_id, state=SagaState.INVENTORY_RESERVED)
        _patch_order(client, order_id, "COMPLETED")
        update_saga(conn, saga_id, state=SagaState.COMPLETED, detail={"finished": True})
        slog("Inventory reserved — order COMPLETED")
        return {"saga_id": saga_id, "state": "COMPLETED", "order_id": order_id}


def _patch_order(client: httpx.Client, order_id: str, status: str) -> None:
    r = client.patch(
        f"{ORDER_URL}/orders/{order_id}/status",
        json={"status": status},
        headers=ORCH_HEADERS,
    )
    r.raise_for_status()
