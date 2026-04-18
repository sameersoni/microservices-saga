"""SQLite persistence for order service."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

_lock = threading.Lock()


class OrderStatus(str, Enum):
    CREATED = "CREATED"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class OrderRow:
    id: str
    correlation_id: str
    status: OrderStatus
    amount_cents: int
    items_json: str
    created_at: str
    updated_at: str


def connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            correlation_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            items_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_orders_correlation ON orders(correlation_id);

        CREATE TABLE IF NOT EXISTS processed_events (
            event_id TEXT PRIMARY KEY,
            correlation_id TEXT NOT NULL,
            received_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


@contextmanager
def get_conn(path: str):
    conn = connect(path)
    try:
        init_db(conn)
        yield conn
    finally:
        conn.close()


def insert_order(
    conn: sqlite3.Connection,
    *,
    order_id: str,
    correlation_id: str,
    amount_cents: int,
    items: list[dict[str, Any]],
) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            """
            INSERT INTO orders (id, correlation_id, status, amount_cents, items_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                correlation_id,
                OrderStatus.CREATED.value,
                amount_cents,
                json.dumps(items),
                now,
                now,
            ),
        )
        conn.commit()


def update_status(conn: sqlite3.Connection, order_id: str, status: OrderStatus) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            "UPDATE orders SET status = ?, updated_at = ? WHERE id = ?",
            (status.value, now, order_id),
        )
        conn.commit()


def get_by_id(conn: sqlite3.Connection, order_id: str) -> OrderRow | None:
    with _lock:
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    if not row:
        return None
    return OrderRow(
        id=row["id"],
        correlation_id=row["correlation_id"],
        status=OrderStatus(row["status"]),
        amount_cents=row["amount_cents"],
        items_json=row["items_json"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def get_by_correlation(conn: sqlite3.Connection, correlation_id: str) -> OrderRow | None:
    with _lock:
        row = conn.execute(
            "SELECT * FROM orders WHERE correlation_id = ?", (correlation_id,)
        ).fetchone()
    if not row:
        return None
    return OrderRow(
        id=row["id"],
        correlation_id=row["correlation_id"],
        status=OrderStatus(row["status"]),
        amount_cents=row["amount_cents"],
        items_json=row["items_json"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def try_record_event(conn: sqlite3.Connection, event_id: str, correlation_id: str) -> bool:
    """Return True if event is new and recorded (idempotency)."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        try:
            conn.execute(
                "INSERT INTO processed_events (event_id, correlation_id, received_at) VALUES (?, ?, ?)",
                (event_id, correlation_id, now),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            conn.rollback()
            return False


def list_recent_orders(conn: sqlite3.Connection, limit: int = 50) -> list[OrderRow]:
    with _lock:
        rows = conn.execute(
            "SELECT * FROM orders ORDER BY updated_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [
        OrderRow(
            id=r["id"],
            correlation_id=r["correlation_id"],
            status=OrderStatus(r["status"]),
            amount_cents=r["amount_cents"],
            items_json=r["items_json"],
            created_at=r["created_at"],
            updated_at=r["updated_at"],
        )
        for r in rows
    ]
