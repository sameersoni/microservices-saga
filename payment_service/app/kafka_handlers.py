from __future__ import annotations

from saga_common.events import BaseEvent

from app.charge import process_event
from app.config import DATABASE_PATH
from app.db import get_conn


def saga_handler(ev: BaseEvent) -> list[BaseEvent]:
    with get_conn(DATABASE_PATH) as conn:
        return process_event(conn, ev)
