"""Simple exponential backoff retry for async callables."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")

log = logging.getLogger(__name__)


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 5,
    base_delay_s: float = 0.2,
    max_delay_s: float = 10.0,
    jitter: bool = True,
    operation: str = "operation",
) -> T:
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fn()
        except Exception as e:  # noqa: BLE001 — demo retry wrapper
            if attempt >= max_attempts:
                log.warning(
                    "Retry exhausted for %s after %s attempts: %s",
                    operation,
                    attempt,
                    e,
                )
                raise
            delay = min(base_delay_s * (2 ** (attempt - 1)), max_delay_s)
            if jitter:
                delay = delay * (0.5 + random.random())
            log.info(
                "Retry %s attempt %s/%s in %.2fs: %s",
                operation,
                attempt,
                max_attempts,
                delay,
                e,
            )
            await asyncio.sleep(delay)
