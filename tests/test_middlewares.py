import asyncio
import logging

import pytest
from pydantic import BaseModel
from redis.asyncio import Redis

from thredis import Thredis, StreamMessage
from thredis.middlewares import timeout, trace, error_handler
from thredis.subscriber import StreamSubscriber


class Order(BaseModel):
    id: str


async def _start(app: Thredis, redis_url: str) -> None:
    await app._ensure_publisher()
    for sub in app._subscribers:
        sub.middlewares = app._middlewares + sub.middlewares
        sub.app = app
        from thredis.middleware import build_chain
        sub._chain = build_chain(sub.handler, sub.middlewares, threaded=sub.threaded)
        await sub.start(redis_url)


@pytest.mark.asyncio
async def test_timeout_cancels_slow_handler(redis: Redis, redis_url: str) -> None:
    processed: list[str] = []

    app = Thredis(redis_url=redis_url, middlewares=[timeout(0.5)])

    @app.subscriber(stream="mw-timeout", group="mw-timeout-grp")
    async def handle(msg: StreamMessage[Order]) -> None:
        await asyncio.sleep(5)  # way longer than timeout
        processed.append(msg.body.id)

    await _start(app, redis_url)
    await app.publish("mw-timeout", Order(id="slow"))

    await asyncio.sleep(1.5)

    # Should not have processed (timed out)
    assert "slow" not in processed

    # Message should still be pending (not acked)
    pending = await redis.xpending("mw-timeout", "mw-timeout-grp")
    assert pending["pending"] >= 1

    await app.shutdown()


@pytest.mark.asyncio
async def test_timeout_passes_fast_handler(redis: Redis, redis_url: str) -> None:
    processed: list[str] = []

    app = Thredis(redis_url=redis_url, middlewares=[timeout(5.0)])

    @app.subscriber(stream="mw-timeout-fast", group="mw-timeout-fast-grp")
    async def handle(msg: StreamMessage[Order]) -> None:
        processed.append(msg.body.id)

    await _start(app, redis_url)
    await app.publish("mw-timeout-fast", Order(id="fast"))

    for _ in range(30):
        if processed:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert processed == ["fast"]


@pytest.mark.asyncio
async def test_trace_logs(redis: Redis, redis_url: str) -> None:
    log_records: list[str] = []

    handler_obj = logging.Handler()
    handler_obj.emit = lambda record: log_records.append(record.getMessage())  # type: ignore[assignment]
    logging.getLogger("thredis").addHandler(handler_obj)

    try:
        app = Thredis(redis_url=redis_url, middlewares=[trace()])

        @app.subscriber(stream="mw-trace", group="mw-trace-grp")
        async def handle(msg: StreamMessage[Order]) -> None:
            pass

        await _start(app, redis_url)
        await app.publish("mw-trace", Order(id="traced"))

        for _ in range(30):
            if any("done" in r for r in log_records):
                break
            await asyncio.sleep(0.1)

        await app.shutdown()

        log_text = "\n".join(log_records)
        assert "Order" in log_text
        assert "done" in log_text
    finally:
        logging.getLogger("thredis").removeHandler(handler_obj)


@pytest.mark.asyncio
async def test_trace_logs_failure(redis: Redis, redis_url: str) -> None:
    log_records: list[str] = []

    handler_obj = logging.Handler()
    handler_obj.emit = lambda record: log_records.append(record.getMessage())  # type: ignore[assignment]
    logging.getLogger("thredis").addHandler(handler_obj)

    try:
        app = Thredis(redis_url=redis_url, middlewares=[trace()])

        @app.subscriber(stream="mw-trace-fail", group="mw-trace-fail-grp")
        async def handle(msg: StreamMessage[Order]) -> None:
            raise ValueError("boom")

        await _start(app, redis_url)
        await app.publish("mw-trace-fail", Order(id="fail"))
        await asyncio.sleep(1.0)

        await app.shutdown()

        log_text = "\n".join(log_records)
        assert "failed" in log_text
    finally:
        logging.getLogger("thredis").removeHandler(handler_obj)


@pytest.mark.asyncio
async def test_error_handler_sync_callback(redis: Redis, redis_url: str) -> None:
    errors: list[tuple[str, str]] = []

    def on_error(msg: StreamMessage, exc: Exception) -> None:
        errors.append((msg.body.id, str(exc)))

    app = Thredis(redis_url=redis_url, middlewares=[error_handler(on_error)])

    @app.subscriber(stream="mw-err", group="mw-err-grp")
    async def handle(msg: StreamMessage[Order]) -> None:
        raise RuntimeError("something broke")

    await _start(app, redis_url)
    await app.publish("mw-err", Order(id="bad"))

    for _ in range(30):
        if errors:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()

    assert len(errors) == 1
    assert errors[0][0] == "bad"
    assert "something broke" in errors[0][1]


@pytest.mark.asyncio
async def test_error_handler_async_callback(redis: Redis, redis_url: str) -> None:
    errors: list[str] = []

    async def on_error(msg: StreamMessage, exc: Exception) -> None:
        errors.append(str(exc))

    app = Thredis(redis_url=redis_url, middlewares=[error_handler(on_error)])

    @app.subscriber(stream="mw-err-async", group="mw-err-async-grp")
    async def handle(msg: StreamMessage[Order]) -> None:
        raise ValueError("async error")

    await _start(app, redis_url)
    await app.publish("mw-err-async", Order(id="bad"))

    for _ in range(30):
        if errors:
            break
        await asyncio.sleep(0.1)

    await app.shutdown()
    assert "async error" in errors[0]


@pytest.mark.asyncio
async def test_error_handler_doesnt_swallow(redis: Redis, redis_url: str) -> None:
    """Error handler calls the callback but still re-raises, so the message stays in PEL."""
    errors: list[str] = []

    def on_error(msg: StreamMessage, exc: Exception) -> None:
        errors.append(str(exc))

    app = Thredis(redis_url=redis_url, middlewares=[error_handler(on_error)])

    @app.subscriber(stream="mw-err-pel", group="mw-err-pel-grp")
    async def handle(msg: StreamMessage[Order]) -> None:
        raise RuntimeError("fail")

    await _start(app, redis_url)
    await app.publish("mw-err-pel", Order(id="x"))

    await asyncio.sleep(1.0)

    assert len(errors) >= 1

    # Check PEL before shutdown (stop() deletes the consumer which clears PEL)
    pending = await redis.xpending("mw-err-pel", "mw-err-pel-grp")
    assert pending["pending"] >= 1

    await app.shutdown()
