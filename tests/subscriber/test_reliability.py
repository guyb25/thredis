import asyncio
import time

import pytest
from redis.asyncio import Redis

from thredis.subscriber import StreamSubscriber
from thredis.message import StreamMessage
from thredis.publisher import StreamPublisher

from .conftest import OrderMessage


@pytest.mark.asyncio
async def test_failed_messages_stay_in_pel(redis: Redis, redis_url: str) -> None:
    """Messages that fail should NOT be acked and remain in PEL."""
    processed: list[str] = []

    async def failing_handler(msg: StreamMessage[OrderMessage]) -> None:
        if msg.body.id == "order-fail":
            raise ValueError("Intentional failure")
        processed.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-failures",
        group="test-group",
        handler=failing_handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    await pub.publish("test-failures", OrderMessage(id="order-ok", amount=1.0))
    await pub.publish("test-failures", OrderMessage(id="order-fail", amount=2.0))

    for _ in range(30):
        if len(processed) >= 1:
            break
        await asyncio.sleep(0.1)

    await asyncio.sleep(0.3)

    assert "order-ok" in processed
    assert "order-fail" not in processed

    pending = await redis.xpending(name="test-failures", groupname="test-group")
    assert pending["pending"] >= 1

    await sub.stop(timeout=5.0)


@pytest.mark.asyncio
async def test_multiple_failures_dont_block(redis: Redis, redis_url: str) -> None:
    """Multiple failures should not stop the consumer from processing subsequent messages."""
    processed: list[str] = []

    async def sometimes_failing_handler(msg: StreamMessage[OrderMessage]) -> None:
        idx = int(msg.body.id.split("-")[1])
        if idx < 5:
            raise ValueError(f"Intentional failure for {msg.body.id}")
        processed.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-multi-fail", group="test-multi-fail-group",
        handler=sometimes_failing_handler, model_type=OrderMessage, concurrency=10, batch_size=10,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(10):
        await pub.publish("test-multi-fail", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(50):
        if len(processed) >= 5:
            break
        await asyncio.sleep(0.1)

    pending = await redis.xpending("test-multi-fail", "test-multi-fail-group")
    await sub.stop(timeout=5.0)

    assert set(processed) == {f"order-{i}" for i in range(5, 10)}
    assert pending["pending"] == 5


@pytest.mark.asyncio
async def test_out_of_order_completion(redis: Redis, redis_url: str) -> None:
    """Messages complete out of order - verify all ack correctly."""
    completion_order: list[str] = []

    async def variable_speed_handler(msg: StreamMessage[OrderMessage]) -> None:
        delay = (5 - int(msg.body.id.split("-")[1])) * 0.1
        await asyncio.sleep(delay)
        completion_order.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-ooo", group="test-ooo-group",
        handler=variable_speed_handler, model_type=OrderMessage, concurrency=5, batch_size=5,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(5):
        await pub.publish("test-ooo", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(50):
        if len(completion_order) >= 5:
            break
        await asyncio.sleep(0.1)

    pending = await redis.xpending("test-ooo", "test-ooo-group")
    await sub.stop(timeout=5.0)

    assert len(completion_order) == 5
    assert set(completion_order) == {f"order-{i}" for i in range(5)}
    assert completion_order.index("order-4") < completion_order.index("order-0")
    assert pending["pending"] == 0


@pytest.mark.asyncio
async def test_graceful_shutdown_under_load(redis: Redis, redis_url: str) -> None:
    """Publish many messages, shut down mid-processing. No messages lost."""
    processed: list[str] = []
    total_messages = 50

    async def slow_handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.2)
        processed.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-shutdown-load", group="test-shutdown-group",
        handler=slow_handler, model_type=OrderMessage, concurrency=5, batch_size=10,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(total_messages):
        await pub.publish("test-shutdown-load", OrderMessage(id=f"order-{i}", amount=float(i)))

    await asyncio.sleep(0.5)
    await sub.stop(timeout=5.0)

    processed_count = len(processed)
    pending_info = await redis.xpending("test-shutdown-load", "test-shutdown-group")
    pending_count = pending_info["pending"]
    stream_length = await redis.xlen("test-shutdown-load")

    assert processed_count > 0
    assert processed_count < total_messages
    assert stream_length == total_messages
    assert processed_count + pending_count <= total_messages
