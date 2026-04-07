import asyncio
import logging

import pytest
from redis.asyncio import Redis

from thredis import Thredis, StreamMessage

from .conftest import Notification, _start_app_consumers


@pytest.mark.asyncio
async def test_app_subscriber_and_publish(redis: Redis, redis_url: str) -> None:
    """End-to-end: register subscriber via decorator, publish, verify processing."""
    received: list[str] = []
    app = Thredis(redis_url=redis_url)

    @app.subscriber(stream="notifications", group="notif-svc", concurrency=3)
    async def handle(msg: StreamMessage[Notification]) -> None:
        received.append(msg.body.user_id)

    await _start_app_consumers(app, redis_url)

    await app.publish("notifications", Notification(user_id="user-1", text="hello"))
    await app.publish("notifications", Notification(user_id="user-2", text="world"))

    for _ in range(30):
        if len(received) >= 2:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert set(received) == {"user-1", "user-2"}


@pytest.mark.asyncio
async def test_subscriber_decorator_validates_type() -> None:
    app = Thredis()
    with pytest.raises(TypeError, match="StreamMessage"):

        @app.subscriber(stream="test", group="test")
        async def bad_handler(msg: str) -> None:
            pass


@pytest.mark.asyncio
async def test_subscriber_decorator_rejects_no_hints() -> None:
    app = Thredis()
    with pytest.raises(TypeError, match="type-hinted parameter"):

        @app.subscriber(stream="test", group="test")
        async def bad_handler(msg) -> None:
            pass


@pytest.mark.asyncio
async def test_sync_handler_requires_explicit_threaded() -> None:
    """Sync handler without threaded=True should raise TypeError."""
    app = Thredis()
    with pytest.raises(TypeError, match="sync function"):

        @app.subscriber(stream="test", group="test")
        def sync_handler(msg: StreamMessage[Notification]) -> None:
            pass


@pytest.mark.asyncio
async def test_log_level_configuration() -> None:
    app = Thredis(log_level=logging.DEBUG)
    assert logging.getLogger("thredis").level == logging.DEBUG

    app2 = Thredis(log_level="WARNING")
    assert logging.getLogger("thredis").level == logging.WARNING
