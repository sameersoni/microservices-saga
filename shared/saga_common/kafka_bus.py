"""Kafka producer and topic constants for choreography."""

from __future__ import annotations

import json
import logging

from aiokafka import AIOKafkaProducer

from saga_common.events import BaseEvent

log = logging.getLogger(__name__)

TOPIC_SAGA = "saga.choreography"
TOPIC_DLQ = "saga.dlq"


async def create_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    return producer


async def publish_event(
    producer: AIOKafkaProducer,
    topic: str,
    event: BaseEvent,
    key: bytes | None = None,
) -> None:
    data = event.model_dump(mode="json")
    body = json.dumps(data, default=str).encode("utf-8")
    await producer.send_and_wait(topic, value=body, key=key or event.correlation_id.encode())


async def publish_dlq(
    producer: AIOKafkaProducer,
    *,
    original_topic: str,
    correlation_id: str,
    error: str,
    raw_value: bytes | None,
) -> None:
    payload = {
        "original_topic": original_topic,
        "correlation_id": correlation_id,
        "error": error,
        "raw": raw_value.decode("utf-8", errors="replace") if raw_value else None,
    }
    await producer.send_and_wait(
        TOPIC_DLQ,
        value=json.dumps(payload).encode("utf-8"),
        key=correlation_id.encode(),
    )
