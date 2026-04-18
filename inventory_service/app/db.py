"""SQLite persistence for reservations."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

_lock = threading.Lock()


class ReservationStatus(str, Enum):
    PENDING = "PENDING"
    RESERVED = "RESERVED"
    FAILED = "FAILED"


@dataclass
class ReservationRow:
    id: str
    order_id: str
    correlation_id: str
    status: ReservationStatus
    sku: str
    quantity: int
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
        CREATE TABLE IF NOT EXISTS stock (
            sku TEXT PRIMARY KEY,
            quantity INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reservations (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            correlation_id TEXT NOT NULL,
            status TEXT NOT NULL,
            sku TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(order_id)
        );
        CREATE TABLE IF NOT EXISTS processed_events (
            event_id TEXT PRIMARY KEY,
            correlation_id TEXT NOT NULL,
            received_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def ensure_stock(conn: sqlite3.Connection, sku: str, initial: int) -> None:
    with _lock:
        row = conn.execute("SELECT quantity FROM stock WHERE sku = ?", (sku,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO stock (sku, quantity) VALUES (?, ?)", (sku, initial)
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


def get_reservation_by_order(conn: sqlite3.Connection, order_id: str) -> ReservationRow | None:
    with _lock:
        row = conn.execute(
            "SELECT * FROM reservations WHERE order_id = ?", (order_id,)
        ).fetchone()
    if not row:
        return None
    return ReservationRow(
        id=row["id"],
        order_id=row["order_id"],
        correlation_id=row["correlation_id"],
        status=ReservationStatus(row["status"]),
        sku=row["sku"],
        quantity=row["quantity"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def insert_reservation(
    conn: sqlite3.Connection,
    *,
    reservation_id: str,
    order_id: str,
    correlation_id: str,
    sku: str,
    quantity: int,
    status: ReservationStatus,
) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            """
            INSERT INTO reservations (id, order_id, correlation_id, status, sku, quantity, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                reservation_id,
                order_id,
                correlation_id,
                status.value,
                sku,
                quantity,
                now,
                now,
            ),
        )
        conn.commit()


def try_decrement_stock(conn: sqlite3.Connection, sku: str, qty: int) -> bool:
    with _lock:
        cur = conn.execute("SELECT quantity FROM stock WHERE sku = ?", (sku,))
        r = cur.fetchone()
        if not r or r["quantity"] < qty:
            return False
        conn.execute(
            "UPDATE stock SET quantity = quantity - ? WHERE sku = ? AND quantity >= ?",
            (qty, sku, qty),
        )
        if conn.total_changes == 0:
            conn.rollback()
            return False
        conn.commit()
    return True


def restore_stock(conn: sqlite3.Connection, sku: str, qty: int) -> None:
    with _lock:
        conn.execute(
            "UPDATE stock SET quantity = quantity + ? WHERE sku = ?", (qty, sku)
        )
        conn.commit()
