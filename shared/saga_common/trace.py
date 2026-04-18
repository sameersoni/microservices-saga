"""Optional tracing helpers (simulated distributed trace id)."""

from __future__ import annotations

import secrets


def new_trace_id() -> str:
    return secrets.token_hex(8)


def trace_headers(trace_id: str) -> dict[str, str]:
    return {"X-Trace-Id": trace_id}
