import asyncio
import json

import pytest
from redis.asyncio import Redis

from thredis import Thredis, StreamMessage

from .conftest import Notification, _start_app_consumers


@pytest.mark.asyncio
async def test_status_method(redis: Redis, redis_url: str) -> None:
    """app.status() should return health info about consumers and Redis."""
    app = Thredis(redis_url=redis_url)

    @app.subscriber(stream="status-test", group="status-svc", concurrency=5)
    async def handle(msg: StreamMessage[Notification]) -> None:
        pass

    await _start_app_consumers(app, redis_url)

    status = await app.status()

    assert status["healthy"] is True
    assert status["redis"] is True
    assert status["consumers"] == 1
    assert len(status["consumer_details"]) == 1

    detail = status["consumer_details"][0]
    assert detail["stream"] == "status-test"
    assert detail["group"] == "status-svc"
    assert detail["running"] is True
    assert detail["concurrency"] == 5
    assert detail["inflight"] == 0

    await app.shutdown()


@pytest.mark.asyncio
async def test_health_endpoint(redis: Redis, redis_url: str) -> None:
    """Built-in health server should respond to HTTP GET with status JSON."""
    app = Thredis(redis_url=redis_url, health_port=18080)

    @app.subscriber(stream="health-test", group="health-svc")
    async def handle(msg: StreamMessage[Notification]) -> None:
        pass

    await _start_app_consumers(app, redis_url)

    # Start health server manually (normally done in app.run())
    from thredis._health import HealthServer
    app._health_server = HealthServer(18080, app.status)
    await app._health_server.start()

    # Make an HTTP request to the health endpoint
    reader, writer = await asyncio.open_connection("127.0.0.1", 18080)
    writer.write(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
    await writer.drain()

    response = await asyncio.wait_for(reader.read(4096), timeout=5.0)
    writer.close()
    await writer.wait_closed()

    # Parse response
    response_str = response.decode()
    assert "200 OK" in response_str

    # Extract JSON body (after the double newline)
    body = response_str.split("\r\n\r\n", 1)[1]
    data = json.loads(body)

    assert data["healthy"] is True
    assert data["redis"] is True
    assert data["consumers"] == 1

    await app.shutdown()


@pytest.mark.asyncio
async def test_status_unhealthy_when_no_consumers(redis: Redis, redis_url: str) -> None:
    """status() should report unhealthy when no consumers are running."""
    app = Thredis(redis_url=redis_url)
    await app._ensure_publisher()

    status = await app.status()
    assert status["healthy"] is False
    assert status["consumers"] == 0

    if app._redis:
        await app._redis.aclose()
