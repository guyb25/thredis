import asyncio

import pytest
from redis.asyncio import Redis

from thredis import Thredis, StreamMessage

from .conftest import Notification, _start_app_consumers


@pytest.mark.asyncio
async def test_app_middleware(redis: Redis, redis_url: str) -> None:
    """Verify middleware chain executes around the handler."""
    calls: list[str] = []

    async def logging_middleware(msg, call_next):
        calls.append("before")
        await call_next(msg)
        calls.append("after")

    app = Thredis(redis_url=redis_url, middlewares=[logging_middleware])

    @app.subscriber(stream="mw-test", group="mw-group", concurrency=1)
    async def handle(msg: StreamMessage[Notification]) -> None:
        calls.append("handler")

    await _start_app_consumers(app, redis_url)
    await app.publish("mw-test", Notification(user_id="u1", text="test"))

    for _ in range(30):
        if len(calls) >= 3:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert calls == ["before", "handler", "after"]


@pytest.mark.asyncio
async def test_middleware_can_read_headers(redis: Redis, redis_url: str) -> None:
    """Middleware should access headers via msg.headers on the StreamMessage."""
    captured_in_mw: list[dict] = []

    async def header_middleware(msg, call_next):
        captured_in_mw.append(dict(msg.headers))
        await call_next(msg)

    app = Thredis(redis_url=redis_url, middlewares=[header_middleware])

    @app.subscriber(stream="mw-hdr-test", group="mw-hdr-group", concurrency=1)
    async def handle(msg: StreamMessage[Notification]) -> None:
        pass

    await _start_app_consumers(app, redis_url)
    await app.publish(
        "mw-hdr-test",
        Notification(user_id="u1", text="test"),
        headers={"source": "test-suite"},
    )

    for _ in range(30):
        if len(captured_in_mw) >= 1:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert captured_in_mw[0] == {"source": "test-suite"}
