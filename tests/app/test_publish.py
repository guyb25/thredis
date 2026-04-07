import asyncio

import pytest
from redis.asyncio import Redis

from thredis import Thredis, StreamMessage, publish

from .conftest import Notification, _start_app_consumers


@pytest.mark.asyncio
async def test_publish_with_headers(redis: Redis, redis_url: str) -> None:
    """Headers should round-trip through publish -> handler -> msg.headers."""
    captured: list[dict] = []

    app = Thredis(redis_url=redis_url)

    @app.subscriber(stream="headers-test", group="hdr-group", concurrency=1)
    async def handle(msg: StreamMessage[Notification]) -> None:
        captured.append(dict(msg.headers))

    await _start_app_consumers(app, redis_url)

    await app.publish(
        "headers-test",
        Notification(user_id="u1", text="hi"),
        headers={"correlation_id": "abc-123", "trace_id": "xyz-789"},
    )

    for _ in range(30):
        if len(captured) >= 1:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()

    assert len(captured) == 1
    assert captured[0] == {"correlation_id": "abc-123", "trace_id": "xyz-789"}


@pytest.mark.asyncio
async def test_publish_without_headers(redis: Redis, redis_url: str) -> None:
    """Messages without headers should have empty headers dict."""
    captured: list[dict] = []

    app = Thredis(redis_url=redis_url)

    @app.subscriber(stream="no-headers-test", group="nhdr-group", concurrency=1)
    async def handle(msg: StreamMessage[Notification]) -> None:
        captured.append(dict(msg.headers))

    await _start_app_consumers(app, redis_url)
    await app.publish("no-headers-test", Notification(user_id="u1", text="hi"))

    for _ in range(30):
        if len(captured) >= 1:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()

    assert len(captured) == 1
    assert captured[0] == {}


@pytest.mark.asyncio
async def test_context_publish(redis: Redis, redis_url: str) -> None:
    """publish() from inside a handler should work without app reference."""
    received_downstream: list[str] = []

    app = Thredis(redis_url=redis_url)

    @app.subscriber(stream="ctx-orders", group="ctx-order-svc", concurrency=1)
    async def handle_order(msg: StreamMessage[Notification]) -> None:
        await publish(
            "ctx-downstream",
            Notification(user_id=msg.body.user_id, text="processed"),
        )

    @app.subscriber(stream="ctx-downstream", group="ctx-down-svc", concurrency=1)
    async def handle_downstream(msg: StreamMessage[Notification]) -> None:
        received_downstream.append(msg.body.user_id)

    await _start_app_consumers(app, redis_url)
    await app.publish("ctx-orders", Notification(user_id="user-ctx", text="test"))

    for _ in range(50):
        if len(received_downstream) >= 1:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert received_downstream == ["user-ctx"]


@pytest.mark.asyncio
async def test_context_publish_from_threaded_handler(redis: Redis, redis_url: str) -> None:
    """publish() from a threaded handler should work with its own Redis connection."""
    received_downstream: list[str] = []

    app = Thredis(redis_url=redis_url)

    @app.subscriber(stream="ctx-threaded-orders", group="ctx-thr-svc", concurrency=1, threaded=True)
    async def handle_order(msg: StreamMessage[Notification]) -> None:
        import time
        time.sleep(0.1)  # Simulate CPU work - proves we're in a thread
        await publish(
            "ctx-threaded-downstream",
            Notification(user_id=msg.body.user_id, text="processed-in-thread"),
        )

    @app.subscriber(stream="ctx-threaded-downstream", group="ctx-thr-down-svc", concurrency=1)
    async def handle_downstream(msg: StreamMessage[Notification]) -> None:
        received_downstream.append(msg.body.user_id)

    await _start_app_consumers(app, redis_url)
    await app.publish("ctx-threaded-orders", Notification(user_id="user-thr", text="test"))

    for _ in range(50):
        if len(received_downstream) >= 1:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert received_downstream == ["user-thr"]


@pytest.mark.asyncio
async def test_context_publish_outside_handler() -> None:
    """publish() outside a handler should raise RuntimeError."""
    with pytest.raises(RuntimeError, match="outside of a thredis handler context"):
        await publish("test", Notification(user_id="u1", text="fail"))
