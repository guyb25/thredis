import asyncio

import pytest
from redis.asyncio import Redis

from thredis.subscriber import StreamSubscriber
from thredis.message import StreamMessage
from thredis.publisher import StreamPublisher

from .conftest import OrderMessage


@pytest.mark.asyncio
async def test_autoclaim_recovers_from_dead_consumer(redis: Redis, redis_url: str) -> None:
    """Consumer B should autoclaim messages abandoned by dead consumer A."""
    received_by_b: list[str] = []

    async def noop_handler(msg: StreamMessage[OrderMessage]) -> None:
        await asyncio.sleep(60)

    sub_a = StreamSubscriber(
        stream="test-autoclaim",
        group="test-autoclaim-group",
        handler=noop_handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
    )
    await sub_a.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(3):
        await pub.publish("test-autoclaim", OrderMessage(id=f"order-{i}", amount=float(i)))

    await asyncio.sleep(0.5)

    # Kill consumer A without XGROUP DELCONSUMER
    sub_a._active = False
    if sub_a._loop_task:
        sub_a._loop_task.cancel()
        try:
            await sub_a._loop_task
        except asyncio.CancelledError:
            pass
    for task in list(sub_a._tasks):
        task.cancel()
    await asyncio.gather(*sub_a._tasks, return_exceptions=True)

    pending = await redis.xpending("test-autoclaim", "test-autoclaim-group")
    assert pending["pending"] == 3

    async def handler_b(msg: StreamMessage[OrderMessage]) -> None:
        received_by_b.append(msg.body.id)

    sub_b = StreamSubscriber(
        stream="test-autoclaim",
        group="test-autoclaim-group",
        handler=handler_b,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
        claim_idle_after=500,
    )
    sub_b._maintenance._last_claim_time = 0.0
    await sub_b.start(redis_url)

    for _ in range(50):
        if len(received_by_b) >= 3:
            break
        await asyncio.sleep(0.2)

    await sub_b.stop(timeout=5.0)
    if sub_a._redis:
        await sub_a._redis.aclose()

    assert set(received_by_b) == {"order-0", "order-1", "order-2"}


@pytest.mark.asyncio
async def test_max_retries_discards_poison_message(redis: Redis, redis_url: str) -> None:
    """Messages exceeding max_retries should be acked (removed from PEL)."""
    processed: list[str] = []

    async def always_fails(msg: StreamMessage[OrderMessage]) -> None:
        raise ValueError("poison message")

    # Consumer A: reads and fails, leaving messages in PEL
    sub_a = StreamSubscriber(
        stream="test-retries",
        group="test-retries-group",
        handler=always_fails,
        model_type=OrderMessage,
        concurrency=1,
        batch_size=1,
    )
    await sub_a.start(redis_url)

    pub = StreamPublisher(redis)
    await pub.publish("test-retries", OrderMessage(id="poison", amount=0.0))

    # Wait for consumer A to attempt and fail
    await asyncio.sleep(1.0)

    # Kill consumer A without cleanup (messages stay in PEL)
    sub_a._active = False
    if sub_a._loop_task:
        sub_a._loop_task.cancel()
        try:
            await sub_a._loop_task
        except asyncio.CancelledError:
            pass
    for task in list(sub_a._tasks):
        task.cancel()
    await asyncio.gather(*sub_a._tasks, return_exceptions=True)

    # Verify message is pending with delivery count >= 1
    pending = await redis.xpending("test-retries", "test-retries-group")
    assert pending["pending"] >= 1

    # Consumer B: has max_retries=1, should discard the poison message
    async def handler_b(msg: StreamMessage[OrderMessage]) -> None:
        processed.append(msg.body.id)

    sub_b = StreamSubscriber(
        stream="test-retries",
        group="test-retries-group",
        handler=handler_b,
        model_type=OrderMessage,
        concurrency=1,
        batch_size=1,
        claim_idle_after=500,
        max_retries=1,
    )
    sub_b._maintenance._last_claim_time = 0.0
    await sub_b.start(redis_url)

    # Wait for autoclaim + filtering
    await asyncio.sleep(3.0)

    await sub_b.stop(timeout=5.0)
    if sub_a._redis:
        await sub_a._redis.aclose()

    # Poison message should NOT have been processed by B
    assert "poison" not in processed

    # PEL should be empty - message was acked as dead
    pending_after = await redis.xpending("test-retries", "test-retries-group")
    assert pending_after["pending"] == 0


@pytest.mark.asyncio
async def test_max_retries_with_dead_letter_stream(redis: Redis, redis_url: str) -> None:
    """Messages exceeding max_retries should be moved to the dead letter stream."""

    async def always_fails(msg: StreamMessage[OrderMessage]) -> None:
        raise ValueError("poison")

    # Consumer A: reads and fails
    sub_a = StreamSubscriber(
        stream="test-dlq",
        group="test-dlq-group",
        handler=always_fails,
        model_type=OrderMessage,
        concurrency=1,
        batch_size=1,
    )
    await sub_a.start(redis_url)

    pub = StreamPublisher(redis)
    await pub.publish(
        "test-dlq",
        OrderMessage(id="poison", amount=0.0),
        headers={"correlation_id": "cid-123"},
    )

    await asyncio.sleep(1.0)

    # Kill consumer A
    sub_a._active = False
    if sub_a._loop_task:
        sub_a._loop_task.cancel()
        try:
            await sub_a._loop_task
        except asyncio.CancelledError:
            pass
    for task in list(sub_a._tasks):
        task.cancel()
    await asyncio.gather(*sub_a._tasks, return_exceptions=True)

    # Consumer B: max_retries=1, dead_letter_stream configured
    async def handler_b(msg: StreamMessage[OrderMessage]) -> None:
        pass

    sub_b = StreamSubscriber(
        stream="test-dlq",
        group="test-dlq-group",
        handler=handler_b,
        model_type=OrderMessage,
        concurrency=1,
        batch_size=1,
        claim_idle_after=500,
        max_retries=1,
        dead_letter_stream="test-dlq-dead",
    )
    sub_b._maintenance._last_claim_time = 0.0
    await sub_b.start(redis_url)

    await asyncio.sleep(3.0)
    await sub_b.stop(timeout=5.0)
    if sub_a._redis:
        await sub_a._redis.aclose()

    # Dead letter stream should have the poison message
    dead_entries = await redis.xrange("test-dlq-dead")
    assert len(dead_entries) == 1

    _, dead_fields = dead_entries[0]
    # Should have original data + dead letter metadata
    assert b"_data" in dead_fields
    assert b"_dead_source_stream" in dead_fields
    assert dead_fields[b"_dead_source_stream"] == b"test-dlq"
    assert int(dead_fields[b"_dead_delivery_count"]) >= 2

    # Original message should have its headers preserved
    assert b"_meta" in dead_fields

    # PEL should be empty
    pending = await redis.xpending("test-dlq", "test-dlq-group")
    assert pending["pending"] == 0


@pytest.mark.asyncio
async def test_consumer_trims_stream(redis: Redis, redis_url: str) -> None:
    """Consumer with maxlen should trim the stream after processing."""
    received: list[str] = []

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-trim",
        group="test-trim-group",
        handler=handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=10,
        maxlen=20,
    )
    await sub.start(redis_url)

    # Publish 500 messages - enough for approximate trimming to be visible
    pub = StreamPublisher(redis)
    for i in range(500):
        await pub.publish("test-trim", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(150):
        if len(received) >= 500:
            break
        await asyncio.sleep(0.1)

    await asyncio.sleep(0.3)
    await sub.stop(timeout=5.0)

    assert len(received) == 500

    # Redis approximate trimming (~) is intentionally loose - it trims in bulk
    # at macro node boundaries. With maxlen=20, the stream won't be exactly 20
    # but should be significantly less than the 500 we published.
    stream_length = await redis.xlen("test-trim")
    assert stream_length < 200, f"Stream length {stream_length} - trimming didn't work"


@pytest.mark.asyncio
async def test_consumer_without_maxlen_does_not_trim(redis: Redis, redis_url: str) -> None:
    """Consumer without maxlen should leave the stream untrimmed."""
    received: list[str] = []

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-no-trim",
        group="test-no-trim-group",
        handler=handler,
        model_type=OrderMessage,
        concurrency=10,
        batch_size=10,
        # no maxlen
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(50):
        await pub.publish("test-no-trim", OrderMessage(id=f"order-{i}", amount=float(i)))

    for _ in range(50):
        if len(received) >= 50:
            break
        await asyncio.sleep(0.1)

    await sub.stop(timeout=5.0)

    assert len(received) == 50
    stream_length = await redis.xlen("test-no-trim")
    assert stream_length == 50


@pytest.mark.asyncio
async def test_dead_consumer_cleanup(redis: Redis, redis_url: str) -> None:
    """Idle consumer entries with 0 pending should be removed by cleanup."""

    async def handler(msg: StreamMessage[OrderMessage]) -> None:
        pass

    # Consumer A: start, read nothing, then stop without XGROUP DELCONSUMER
    sub_a = StreamSubscriber(
        stream="test-cleanup",
        group="test-cleanup-group",
        handler=handler,
        model_type=OrderMessage,
        concurrency=1,
        batch_size=1,
        claim_idle_after=100,  # 100ms - very short for testing
    )
    await sub_a.start(redis_url)
    consumer_a_name = sub_a.consumer_name

    # Publish and let A consume (so it registers with the group)
    pub = StreamPublisher(redis)
    await pub.publish("test-cleanup", OrderMessage(id="msg-1", amount=1.0))
    await asyncio.sleep(0.5)

    # Kill A without proper cleanup (don't call stop - that does DELCONSUMER)
    sub_a._active = False
    if sub_a._loop_task:
        sub_a._loop_task.cancel()
        try:
            await sub_a._loop_task
        except asyncio.CancelledError:
            pass
    for task in list(sub_a._tasks):
        task.cancel()
    await asyncio.gather(*sub_a._tasks, return_exceptions=True)

    # Verify A's consumer entry exists
    consumers = await redis.xinfo_consumers("test-cleanup", "test-cleanup-group")
    consumer_names = [
        c["name"].decode() if isinstance(c["name"], bytes) else c["name"]
        for c in consumers
    ]
    assert consumer_a_name in consumer_names

    # Consumer B: short cleanup interval so it runs quickly
    sub_b = StreamSubscriber(
        stream="test-cleanup",
        group="test-cleanup-group",
        handler=handler,
        model_type=OrderMessage,
        concurrency=1,
        batch_size=1,
        claim_idle_after=100,
    )
    # Override cleanup interval to 1 second
    sub_b._maintenance._cleanup_interval = 1.0
    sub_b._maintenance._last_cleanup_time = 0.0
    await sub_b.start(redis_url)

    # Wait for cleanup to run (idle threshold = claim_idle_after * 2 = 200ms)
    await asyncio.sleep(3.0)

    # Verify A's consumer entry is gone
    consumers_after = await redis.xinfo_consumers("test-cleanup", "test-cleanup-group")
    consumer_names_after = [
        c["name"].decode() if isinstance(c["name"], bytes) else c["name"]
        for c in consumers_after
    ]
    assert consumer_a_name not in consumer_names_after
    assert sub_b.consumer_name in consumer_names_after

    await sub_b.stop(timeout=5.0)
    if sub_a._redis:
        await sub_a._redis.aclose()
