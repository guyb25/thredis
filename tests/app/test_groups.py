import asyncio

import pytest
from redis.asyncio import Redis

from thredis import Thredis, StreamMessage, SubscriberGroup

from .conftest import Notification, _start_app_consumers


@pytest.mark.asyncio
async def test_subscriber_group(redis: Redis, redis_url: str) -> None:
    """Subscribers in a group should inherit group-level config."""
    received: list[str] = []

    app = Thredis(redis_url=redis_url)

    orders = SubscriberGroup(group="grp-test-svc", concurrency=3)

    @orders.subscriber(stream="grp-orders")
    async def handle_order(msg: StreamMessage[Notification]) -> None:
        received.append(f"order:{msg.body.user_id}")

    @orders.subscriber(stream="grp-refunds")
    async def handle_refund(msg: StreamMessage[Notification]) -> None:
        received.append(f"refund:{msg.body.user_id}")

    app.include_group(orders)

    await _start_app_consumers(app, redis_url)

    await app.publish("grp-orders", Notification(user_id="u1", text="order"))
    await app.publish("grp-refunds", Notification(user_id="u2", text="refund"))

    for _ in range(30):
        if len(received) >= 2:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert set(received) == {"order:u1", "refund:u2"}


@pytest.mark.asyncio
async def test_subscriber_group_middleware(redis: Redis, redis_url: str) -> None:
    """Group middleware should run after app middleware, before handler."""
    calls: list[str] = []

    async def app_mw(msg, call_next):
        calls.append("app")
        await call_next(msg)

    async def group_mw(msg, call_next):
        calls.append("group")
        await call_next(msg)

    app = Thredis(redis_url=redis_url, middlewares=[app_mw])

    group = SubscriberGroup(group="grp-mw-svc", middlewares=[group_mw])

    @group.subscriber(stream="grp-mw-test")
    async def handle(msg: StreamMessage[Notification]) -> None:
        calls.append("handler")

    app.include_group(group)
    await _start_app_consumers(app, redis_url)

    await app.publish("grp-mw-test", Notification(user_id="u1", text="test"))

    for _ in range(30):
        if len(calls) >= 3:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert calls == ["app", "group", "handler"]


@pytest.mark.asyncio
async def test_subscriber_group_override(redis: Redis, redis_url: str) -> None:
    """Per-subscriber values should override group defaults."""
    received: list[str] = []

    app = Thredis(redis_url=redis_url)

    group = SubscriberGroup(group="grp-override-svc", concurrency=10)

    # This one uses a different group name
    @group.subscriber(stream="grp-override-test", group="grp-override-custom")
    async def handle(msg: StreamMessage[Notification]) -> None:
        received.append(msg.body.user_id)

    app.include_group(group)

    await _start_app_consumers(app, redis_url)
    await app.publish("grp-override-test", Notification(user_id="u1", text="test"))

    for _ in range(30):
        if len(received) >= 1:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert received == ["u1"]


@pytest.mark.asyncio
async def test_subscriber_group_requires_group_name() -> None:
    """Group without group name and subscriber without group name should raise."""
    group = SubscriberGroup()  # no group name

    @group.subscriber(stream="test")  # no group name here either
    async def handle(msg: StreamMessage[Notification]) -> None:
        pass

    with pytest.raises(ValueError, match="no group"):
        group._build_configs()
