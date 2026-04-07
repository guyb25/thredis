import asyncio
import time

import pytest
from redis.asyncio import Redis

from thredis.subscriber import StreamSubscriber
from thredis.message import StreamMessage
from thredis.publisher import StreamPublisher

from .conftest import OrderMessage


@pytest.mark.asyncio
async def test_competing_consumers_no_duplicates(redis: Redis, redis_url: str) -> None:
    """Two consumers in the same group should process all messages exactly once."""
    received_a: list[str] = []
    received_b: list[str] = []

    async def handler_a(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.05)
        received_a.append(msg.body.id)

    async def handler_b(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.05)
        received_b.append(msg.body.id)

    sub_a = StreamSubscriber(
        stream="test-competing",
        group="test-competing-group",
        handler=handler_a,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
    )
    sub_b = StreamSubscriber(
        stream="test-competing",
        group="test-competing-group",
        handler=handler_b,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
    )

    await sub_a.start(redis_url)
    await sub_b.start(redis_url)

    pub = StreamPublisher(redis)
    total = 20
    for i in range(total):
        await pub.publish("test-competing", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(100):
        if len(received_a) + len(received_b) >= total:
            break
        await asyncio.sleep(0.1)

    await sub_a.stop(timeout=5.0)
    await sub_b.stop(timeout=5.0)

    all_received = received_a + received_b
    # All messages processed exactly once
    assert len(all_received) == total
    assert len(set(all_received)) == total  # no duplicates
    assert set(all_received) == {f"order-{i}" for i in range(total)}
    # Both consumers should have gotten some messages (not all to one)
    assert len(received_a) > 0, "Consumer A got no messages"
    assert len(received_b) > 0, "Consumer B got no messages"


@pytest.mark.asyncio
async def test_competing_consumers_fast_gets_more(redis: Redis, redis_url: str) -> None:
    """When one consumer is slow, the fast one should pick up more messages."""
    received_fast: list[str] = []
    received_slow: list[str] = []

    async def fast_handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.01)
        received_fast.append(msg.body.id)

    async def slow_handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.3)
        received_slow.append(msg.body.id)

    sub_fast = StreamSubscriber(
        stream="test-fast-slow",
        group="test-fast-slow-group",
        handler=fast_handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
    )
    sub_slow = StreamSubscriber(
        stream="test-fast-slow",
        group="test-fast-slow-group",
        handler=slow_handler,
        model_type=OrderMessage,
        concurrency=1,
        batch_size=1,
    )

    await sub_fast.start(redis_url)
    await sub_slow.start(redis_url)

    pub = StreamPublisher(redis)
    total = 20
    for i in range(total):
        await pub.publish("test-fast-slow", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(100):
        if len(received_fast) + len(received_slow) >= total:
            break
        await asyncio.sleep(0.1)

    await sub_fast.stop(timeout=5.0)
    await sub_slow.stop(timeout=5.0)

    all_received = received_fast + received_slow
    assert len(all_received) == total
    assert len(set(all_received)) == total
    # Fast consumer should have gotten more
    assert len(received_fast) > len(received_slow), (
        f"Fast got {len(received_fast)}, slow got {len(received_slow)} - "
        f"expected fast to get more"
    )


@pytest.mark.asyncio
async def test_cross_consumer_isolation(redis: Redis, redis_url: str) -> None:
    """A slow subscriber should not affect another subscriber."""
    fast_received: list[str] = []
    slow_received: list[str] = []

    async def fast_handler(msg: StreamMessage[OrderMessage]) -> None:
        fast_received.append(msg.body.id)

    async def slow_handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.5)
        slow_received.append(msg.body.id)

    sub_fast = StreamSubscriber(
        stream="test-iso-fast", group="test-iso-fast-group",
        handler=fast_handler, model_type=OrderMessage, concurrency=5, batch_size=5,
    )
    sub_slow = StreamSubscriber(
        stream="test-iso-slow", group="test-iso-slow-group",
        handler=slow_handler, model_type=OrderMessage, concurrency=1, batch_size=1,
    )

    await sub_fast.start(redis_url)
    await sub_slow.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(5):
        await pub.publish("test-iso-slow", OrderMessage(id=f"slow-{i}", amount=float(i)))
    for i in range(5):
        await pub.publish("test-iso-fast", OrderMessage(id=f"fast-{i}", amount=float(i)))

    start = time.monotonic()
    for _ in range(30):
        if len(fast_received) >= 5:
            break
        await asyncio.sleep(0.1)
    fast_elapsed = time.monotonic() - start

    await sub_fast.stop(timeout=5.0)
    await sub_slow.stop(timeout=5.0)

    assert len(fast_received) == 5
    assert fast_elapsed < 1.0, f"Fast consumer took {fast_elapsed:.2f}s - slow consumer is blocking it"
