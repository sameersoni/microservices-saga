from __future__ import annotations

from saga_common.events import BaseEvent

from app.config import DATABASE_PATH, INITIAL_STOCK
from app.db import ensure_stock, get_conn
from app.reserve import DEFAULT_SKU, process_event


def saga_handler(ev: BaseEvent) -> list[BaseEvent]:
    with get_conn(DATABASE_PATH) as conn:
        ensure_stock(conn, DEFAULT_SKU, INITIAL_STOCK)
        return process_event(conn, ev)
