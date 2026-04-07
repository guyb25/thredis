import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from redis.asyncio import Redis

REDIS_URL = "redis://localhost:6379/15"  # Use DB 15 for tests


@pytest_asyncio.fixture
async def redis() -> AsyncGenerator[Redis, None]:
    client = Redis.from_url(REDIS_URL, decode_responses=False)
    # Flush test database
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
def redis_url() -> str:
    return REDIS_URL
