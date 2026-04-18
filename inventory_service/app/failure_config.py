from __future__ import annotations

import threading

from app.config import INVENTORY_FAILURE_RATE as _DEFAULT

_lock = threading.Lock()
_rate = _DEFAULT


def get_failure_rate() -> float:
    with _lock:
        return _rate


def set_failure_rate(r: float) -> None:
    global _rate
    with _lock:
        _rate = max(0.0, min(1.0, r))
