import pytest
from pydantic import BaseModel
from redis.asyncio import Redis

from thredis.message import DATA_KEY
from thredis.publisher import StreamPublisher


class SampleMessage(BaseModel):
    id: str
    amount: float


@pytest.mark.asyncio
async def test_publish_adds_to_stream(redis: Redis) -> None:
    pub = StreamPublisher(redis)
    msg = SampleMessage(id="order-1", amount=99.99)

    msg_id = await pub.publish("test-stream", msg)

    assert msg_id is not None
    entries = await redis.xrange("test-stream")
    assert len(entries) == 1

    stored_id, fields = entries[0]
    assert stored_id.decode() == msg_id
    assert DATA_KEY in fields

    restored = SampleMessage.model_validate_json(fields[DATA_KEY])
    assert restored.id == "order-1"
    assert restored.amount == 99.99


@pytest.mark.asyncio
async def test_publish_multiple_messages(redis: Redis) -> None:
    pub = StreamPublisher(redis)

    ids = []
    for i in range(5):
        msg_id = await pub.publish(
            "test-stream",
            SampleMessage(id=f"order-{i}", amount=float(i)),
        )
        ids.append(msg_id)

    assert len(set(ids)) == 5

    entries = await redis.xrange("test-stream")
    assert len(entries) == 5


@pytest.mark.asyncio
async def test_publish_with_headers(redis: Redis) -> None:
    pub = StreamPublisher(redis)

    await pub.publish(
        "test-stream",
        SampleMessage(id="order-1", amount=1.0),
        headers={"correlation_id": "abc", "source": "test"},
    )

    entries = await redis.xrange("test-stream")
    assert len(entries) == 1
    _, fields = entries[0]
    assert b"_meta" in fields
