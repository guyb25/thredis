import asyncio
import time

import pytest
from redis.asyncio import Redis

from thredis.subscriber import StreamSubscriber
from thredis.message import StreamMessage
from thredis.publisher import StreamPublisher

from .conftest import OrderMessage


@pytest.mark.asyncio
async def test_backpressure(redis: Redis, redis_url: str) -> None:
    """With concurrency=2 and 6 messages each taking 0.3s, should take ~0.9s."""
    received: list[str] = []

    async def slow_handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.3)
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-backpressure",
        group="test-group",
        handler=slow_handler,
        model_type=OrderMessage,
        concurrency=2,
        batch_size=6,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(6):
        await pub.publish("test-backpressure", OrderMessage(id=f"order-{i}", amount=float(i)))

    start = time.monotonic()
    for _ in range(50):
        if len(received) >= 6:
            break
        await asyncio.sleep(0.1)
    elapsed = time.monotonic() - start

    await sub.stop(timeout=5.0)

    assert len(received) == 6
    assert elapsed >= 0.8, f"Too fast ({elapsed:.2f}s) - backpressure not working"
    assert elapsed < 2.5, f"Too slow ({elapsed:.2f}s) - not concurrent enough"


@pytest.mark.asyncio
async def test_high_volume_concurrent_processing(redis: Redis, redis_url: str) -> None:
    """100 messages with concurrency=20 should process concurrently, not sequentially."""
    received: list[str] = []

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(0.1)
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-volume",
        group="test-volume-group",
        handler=handler,
        model_type=OrderMessage,
        concurrency=20,
        batch_size=20,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    total = 100
    for i in range(total):
        await pub.publish("test-volume", OrderMessage(id=f"order-{i}", amount=float(i)))

    start = time.monotonic()

    for _ in range(100):
        if len(received) >= total:
            break
        await asyncio.sleep(0.1)

    elapsed = time.monotonic() - start
    await sub.stop(timeout=5.0)

    assert len(received) == total
    # Sequential: 100 * 0.1s = 10s. Concurrent (20 slots): ~0.5s. Allow generous margin.
    assert elapsed < 3.0, (
        f"Took {elapsed:.2f}s for {total} messages - "
        f"expected < 3s with concurrency=20"
    )


@pytest.mark.asyncio
async def test_sustained_backpressure_no_task_leak(redis: Redis, redis_url: str) -> None:
    """Under sustained load, task set should never exceed concurrency."""
    max_inflight_seen = 0
    received: list[str] = []

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        nonlocal max_inflight_seen
        current = len(sub._tasks)
        if current > max_inflight_seen:
            max_inflight_seen = current
        await asyncio.sleep(0.1)
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-sustained",
        group="test-sustained-group",
        handler=handler,
        model_type=OrderMessage,
        concurrency=3,
        batch_size=10,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    total = 30
    for i in range(total):
        await pub.publish("test-sustained", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(100):
        if len(received) >= total:
            break
        await asyncio.sleep(0.1)

    await sub.stop(timeout=5.0)

    assert len(received) == total
    # Task set should never have exceeded concurrency + 1
    # (+1 because the check happens inside the task before semaphore state is fully settled)
    assert max_inflight_seen <= sub.concurrency + 1, (
        f"Max inflight was {max_inflight_seen}, expected <= {sub.concurrency + 1}"
    )
