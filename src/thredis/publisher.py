from __future__ import annotations

from typing import Optional

from pydantic import BaseModel
from redis.asyncio import Redis

from thredis.message import serialize


class StreamPublisher:

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(
        self,
        stream: str,
        model: BaseModel,
        headers: Optional[dict[str, str]] = None,
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> str:
        fields = serialize(model, headers=headers)
        msg_id: bytes = await self._redis.xadd(
            name=stream,
            fields=fields,  # type: ignore[arg-type]
            maxlen=maxlen,
            approximate=approximate and maxlen is not None,
        )
        return msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
