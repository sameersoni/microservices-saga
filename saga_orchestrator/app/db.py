"""Orchestrator saga persistence."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

_lock = threading.Lock()


class SagaState(str, Enum):
    PENDING = "PENDING"
    ORDER_CREATED = "ORDER_CREATED"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    COMPENSATING = "COMPENSATING"


@dataclass
class SagaRow:
    saga_id: str
    order_id: str | None
    state: SagaState
    detail_json: str
    created_at: str
    updated_at: str


def connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(path, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sagas (
            saga_id TEXT PRIMARY KEY,
            order_id TEXT,
            state TEXT NOT NULL,
            detail_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS saga_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            saga_id TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
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


def insert_saga(conn: sqlite3.Connection, saga_id: str, detail: dict) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            """
            INSERT INTO sagas (saga_id, order_id, state, detail_json, created_at, updated_at)
            VALUES (?, NULL, ?, ?, ?, ?)
            """,
            (saga_id, SagaState.PENDING.value, json.dumps(detail), now, now),
        )
        conn.commit()


def update_saga(
    conn: sqlite3.Connection,
    saga_id: str,
    *,
    order_id: str | None = None,
    state: SagaState | None = None,
    detail: dict | None = None,
) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        row = conn.execute("SELECT * FROM sagas WHERE saga_id = ?", (saga_id,)).fetchone()
        if not row:
            conn.rollback()
            return
        new_order = order_id if order_id is not None else row["order_id"]
        new_state = state.value if state else row["state"]
        d = json.loads(row["detail_json"])
        if detail is not None:
            d.update(detail)
        conn.execute(
            """
            UPDATE sagas SET order_id = ?, state = ?, detail_json = ?, updated_at = ?
            WHERE saga_id = ?
            """,
            (new_order, new_state, json.dumps(d), now, saga_id),
        )
        conn.commit()


def append_log(conn: sqlite3.Connection, saga_id: str, message: str) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn.execute(
            "INSERT INTO saga_event_log (saga_id, message, created_at) VALUES (?, ?, ?)",
            (saga_id, message, now),
        )
        conn.commit()


def get_saga(conn: sqlite3.Connection, saga_id: str) -> SagaRow | None:
    with _lock:
        row = conn.execute("SELECT * FROM sagas WHERE saga_id = ?", (saga_id,)).fetchone()
    if not row:
        return None
    return SagaRow(
        saga_id=row["saga_id"],
        order_id=row["order_id"],
        state=SagaState(row["state"]),
        detail_json=row["detail_json"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def list_saga_logs(conn: sqlite3.Connection, saga_id: str, limit: int = 200) -> list[str]:
    with _lock:
        rows = conn.execute(
            """
            SELECT message FROM saga_event_log WHERE saga_id = ?
            ORDER BY id ASC LIMIT ?
            """,
            (saga_id, limit),
        ).fetchall()
    return [r["message"] for r in rows]


def list_recent_sagas(conn: sqlite3.Connection, limit: int = 50) -> list[SagaRow]:
    with _lock:
        rows = conn.execute(
            "SELECT * FROM sagas ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    out: list[SagaRow] = []
    for row in rows:
        out.append(
            SagaRow(
                saga_id=row["saga_id"],
                order_id=row["order_id"],
                state=SagaState(row["state"]),
                detail_json=row["detail_json"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        )
    return out
