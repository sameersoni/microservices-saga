from __future__ import annotations

from saga_common.events import BaseEvent

from app.config import DATABASE_PATH
from app.db import get_conn
from app.events_handling import process_order_event


def saga_handler(ev: BaseEvent) -> list[BaseEvent]:
    with get_conn(DATABASE_PATH) as conn:
        return process_order_event(conn, ev)
