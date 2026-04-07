import asyncio
import time

import pytest
from redis.asyncio import Redis

from thredis.subscriber import StreamSubscriber
from thredis.message import StreamMessage
from thredis.publisher import StreamPublisher

from .conftest import OrderMessage


@pytest.mark.asyncio
async def test_consumer_processes_messages(redis: Redis, redis_url: str) -> None:
    """Basic test: publish messages, verify they're all processed."""
    received: list[str] = []

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-orders",
        group="test-group",
        handler=handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(10):
        await pub.publish("test-orders", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(50):
        if len(received) >= 10:
            break
        await asyncio.sleep(0.1)

    await sub.stop(timeout=5.0)

    assert len(received) == 10
    assert set(received) == {f"order-{i}" for i in range(10)}


@pytest.mark.asyncio
async def test_concurrent_processing(redis: Redis, redis_url: str) -> None:
    """With concurrency=5 and 5 messages each taking 0.5s, should be ~0.5s not ~2.5s."""
    received: list[str] = []

    async def slow_handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.5)
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-concurrent",
        group="test-group",
        handler=slow_handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(5):
        await pub.publish("test-concurrent", OrderMessage(id=f"order-{i}", amount=float(i)))

    start = time.monotonic()
    for _ in range(50):
        if len(received) >= 5:
            break
        await asyncio.sleep(0.1)
    elapsed = time.monotonic() - start

    await sub.stop(timeout=5.0)

    assert len(received) == 5
    assert elapsed < 1.5, f"Took {elapsed:.2f}s - messages were processed sequentially!"


@pytest.mark.asyncio
async def test_handler_can_access_headers(redis: Redis, redis_url: str) -> None:
    """Handler should be able to read headers via msg.headers."""
    captured_headers: list[dict] = []

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        captured_headers.append(dict(msg.headers))

    sub = StreamSubscriber(
        stream="test-handler-headers", group="test-hdr-group",
        handler=handler, model_type=OrderMessage, concurrency=1, batch_size=1,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    await pub.publish(
        "test-handler-headers",
        OrderMessage(id="order-1", amount=1.0),
        headers={"correlation_id": "abc-123", "trace_id": "xyz"},
    )

    for _ in range(30):
        if len(captured_headers) >= 1:
            break
        await asyncio.sleep(0.1)

    await sub.stop(timeout=5.0)

    assert captured_headers[0] == {"correlation_id": "abc-123", "trace_id": "xyz"}


@pytest.mark.asyncio
async def test_handler_can_access_metadata(redis: Redis, redis_url: str) -> None:
    """Handler should be able to read msg_id and stream from StreamMessage."""
    captured: list[dict] = []

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        captured.append({"msg_id": msg.msg_id, "stream": msg.stream})

    sub = StreamSubscriber(
        stream="test-handler-meta", group="test-meta-group",
        handler=handler, model_type=OrderMessage, concurrency=1, batch_size=1,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    await pub.publish("test-handler-meta", OrderMessage(id="order-1", amount=1.0))

    for _ in range(30):
        if len(captured) >= 1:
            break
        await asyncio.sleep(0.1)

    await sub.stop(timeout=5.0)

    assert captured[0]["stream"] == "test-handler-meta"
    assert captured[0]["msg_id"]  # Should be a non-empty Redis message ID
