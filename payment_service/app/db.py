"""SQLite persistence for payments."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

_lock = threading.Lock()


class PaymentStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


@dataclass
class PaymentRow:
    id: str
    order_id: str
    correlation_id: str
    status: PaymentStatus
    amount_cents: int
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
        CREATE TABLE IF NOT EXISTS payments (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            correlation_id TEXT NOT NULL,
            status TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(order_id)
        );
        CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);

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


def insert_payment(
    conn: sqlite3.Connection,
    *,
    payment_id: str,
    order_id: str,
    correlation_id: str,
    amount_cents: int,
    status: PaymentStatus,
) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            """
            INSERT INTO payments (id, order_id, correlation_id, status, amount_cents, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (payment_id, order_id, correlation_id, status.value, amount_cents, now, now),
        )
        conn.commit()


def update_payment_status(
    conn: sqlite3.Connection, payment_id: str, status: PaymentStatus
) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            "UPDATE payments SET status = ?, updated_at = ? WHERE id = ?",
            (status.value, now, payment_id),
        )
        conn.commit()


def get_by_order(conn: sqlite3.Connection, order_id: str) -> PaymentRow | None:
    with _lock:
        row = conn.execute(
            "SELECT * FROM payments WHERE order_id = ?", (order_id,)
        ).fetchone()
    if not row:
        return None
    return PaymentRow(
        id=row["id"],
        order_id=row["order_id"],
        correlation_id=row["correlation_id"],
        status=PaymentStatus(row["status"]),
        amount_cents=row["amount_cents"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def try_record_event(conn: sqlite3.Connection, event_id: str, correlation_id: str) -> bool:
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

