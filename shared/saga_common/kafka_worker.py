"""Reusable Kafka consumer loop with retries and DLQ."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from saga_common.events import BaseEvent, parse_event
from saga_common.kafka_bus import TOPIC_DLQ, TOPIC_SAGA, publish_dlq, publish_event
from saga_common.logging_conf import correlation_id_var
from saga_common.retry import retry_async

log = logging.getLogger(__name__)


async def run_saga_consumer(
    *,
    bootstrap_servers: str,
    group_id: str,
    producer: AIOKafkaProducer,
    handler: Callable[[BaseEvent], list[BaseEvent]],
    stop: asyncio.Event,
    max_retries: int = 4,
) -> None:
    """Consume TOPIC_SAGA; handler returns outbound events to publish."""
    consumer = AIOKafkaConsumer(
        TOPIC_SAGA,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        while not stop.is_set():
            records = await consumer.getmany(timeout_ms=800, max_records=32)
            if not records:
                await asyncio.sleep(0.05)
                continue
            for _tp, batch in records.items():
                for msg in batch:
                    if stop.is_set():
                        break
                    await _handle_one_message(
                        consumer,
                        producer,
                        msg.value,
                        handler,
                        max_retries=max_retries,
                    )
    finally:
        await consumer.stop()


async def _handle_one_message(
    consumer,
    producer: AIOKafkaProducer,
    raw: bytes,
    handler: Callable[[BaseEvent], list[BaseEvent]],
    *,
    max_retries: int,
) -> None:
    correlation_id = "-"
    try:
        ev = parse_event(raw)
        correlation_id = ev.correlation_id
        correlation_id_var.set(correlation_id)

        async def once() -> list[BaseEvent]:
            return await asyncio.to_thread(handler, ev)

        outs = await retry_async(
            once,
            max_attempts=max_retries,
            base_delay_s=0.25,
            operation="handle_event",
        )
        for o in outs:
            await publish_event(producer, TOPIC_SAGA, o)
        await consumer.commit()
    except Exception as e:  # noqa: BLE001 — DLQ path
        log.exception("Message failed after retries; sending to DLQ: %s", e)
        try:
            await publish_dlq(
                producer,
                original_topic=TOPIC_SAGA,
                correlation_id=correlation_id,
                error=str(e),
                raw_value=raw,
            )
            await consumer.commit()
        except Exception:
            log.exception("DLQ publish failed")
            raise
