import asyncio
import threading
import time

import pytest
from redis.asyncio import Redis

from thredis.subscriber import StreamSubscriber
from thredis.message import StreamMessage
from thredis.publisher import StreamPublisher

from .conftest import OrderMessage


@pytest.mark.asyncio
async def test_threaded_sync_handler(redis: Redis, redis_url: str) -> None:
    """Sync handler with threaded=True should run in a thread, not block the loop."""
    received: list[str] = []
    thread_ids: list[int] = []
    main_thread = threading.current_thread().ident

    def sync_handler(msg: StreamMessage[OrderMessage]) -> None:
        thread_ids.append(threading.current_thread().ident)
        time.sleep(0.3)
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-threaded",
        group="test-group",
        handler=sync_handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
        threaded=True,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(5):
        await pub.publish("test-threaded", OrderMessage(id=f"order-{i}", amount=float(i)))

    start = time.monotonic()
    for _ in range(50):
        if len(received) >= 5:
            break
        await asyncio.sleep(0.1)
    elapsed = time.monotonic() - start

    await sub.stop(timeout=5.0)

    assert len(received) == 5
    assert elapsed < 1.0, f"Took {elapsed:.2f}s - threaded handlers not running concurrently"
    assert all(tid != main_thread for tid in thread_ids)


@pytest.mark.asyncio
async def test_threaded_async_handler(redis: Redis, redis_url: str) -> None:
    """Async handler with threaded=True gets its own event loop in a thread."""
    received: list[str] = []
    thread_ids: list[int] = []
    main_thread = threading.current_thread().ident

    async def cpu_and_io_handler(msg: StreamMessage[OrderMessage]) -> None:
        thread_ids.append(threading.current_thread().ident)
        time.sleep(0.2)
        await asyncio.sleep(0.1)
        received.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-threaded-async",
        group="test-group",
        handler=cpu_and_io_handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
        threaded=True,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    for i in range(5):
        await pub.publish("test-threaded-async", OrderMessage(id=f"order-{i}", amount=float(i)))

    start = time.monotonic()
    for _ in range(50):
        if len(received) >= 5:
            break
        await asyncio.sleep(0.1)
    elapsed = time.monotonic() - start

    await sub.stop(timeout=5.0)

    assert len(received) == 5
    assert elapsed < 1.0, f"Took {elapsed:.2f}s - threaded async handlers not concurrent"
    assert all(tid != main_thread for tid in thread_ids)


@pytest.mark.asyncio
async def test_threaded_sync_handler_failure(redis: Redis, redis_url: str) -> None:
    """A sync threaded handler that raises should leave the message in PEL."""
    processed: list[str] = []

    def failing_sync_handler(msg: StreamMessage[OrderMessage]) -> None:
        if msg.body.id == "fail":
            raise ValueError("sync failure")
        processed.append(msg.body.id)

    sub = StreamSubscriber(
        stream="test-threaded-fail",
        group="test-threaded-fail-group",
        handler=failing_sync_handler,
        model_type=OrderMessage,
        concurrency=5,
        batch_size=5,
        threaded=True,
    )
    await sub.start(redis_url)

    pub = StreamPublisher(redis)
    await pub.publish("test-threaded-fail", OrderMessage(id="ok", amount=1.0))
    await pub.publish("test-threaded-fail", OrderMessage(id="fail", amount=2.0))

    for _ in range(30):
        if len(processed) >= 1:
            break
        await asyncio.sleep(0.1)

    await asyncio.sleep(0.5)

    assert "ok" in processed
    assert "fail" not in processed

    pending = await redis.xpending("test-threaded-fail", "test-threaded-fail-group")
    assert pending["pending"] >= 1

    await sub.stop(timeout=5.0)


@pytest.mark.asyncio
async def test_threaded_publish_preserves_headers(redis: Redis, redis_url: str) -> None:
    """Headers published from a threaded handler should arrive intact."""
    captured_headers: list[dict] = []

    async def downstream_handler(msg: StreamMessage[OrderMessage]) -> None:
        captured_headers.append(dict(msg.headers))

    # Threaded handler that publishes with headers
    from thredis.context import set_current_app
    from thredis import Thredis, publish

    app = Thredis(redis_url=redis_url)

    @app.subscriber(stream="thr-pub-src", group="thr-pub-src-group", concurrency=1, threaded=True)
    async def threaded_handler(msg: StreamMessage[OrderMessage]) -> None:
        import time
        time.sleep(0.1)  # prove we're in a thread
        await publish(
            "thr-pub-dst",
            OrderMessage(id=f"forwarded-{msg.body.id}", amount=msg.body.amount),
            headers={"correlation_id": "cid-from-thread", "source": "threaded-handler"},
        )

    @app.subscriber(stream="thr-pub-dst", group="thr-pub-dst-group", concurrency=1)
    async def capture(msg: StreamMessage[OrderMessage]) -> None:
        captured_headers.append(dict(msg.headers))

    from thredis.subscriber import StreamSubscriber
    from thredis.middleware import build_chain
    await app._ensure_publisher()
    for sub in app._subscribers:
        sub.middlewares = app._middlewares + sub.middlewares
        sub.app = app
        sub._chain = build_chain(sub.handler, sub.middlewares, threaded=sub.threaded)
        await sub.start(redis_url)

    await app.publish("thr-pub-src", OrderMessage(id="original", amount=1.0))

    for _ in range(50):
        if captured_headers:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()

    assert len(captured_headers) == 1
    assert captured_headers[0]["correlation_id"] == "cid-from-thread"
    assert captured_headers[0]["source"] == "threaded-handler"
