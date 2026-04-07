from __future__ import annotations

import json
import logging
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel, Field, ConfigDict
from redis.asyncio import Redis

T = TypeVar("T", bound=BaseModel)

DATA_KEY = b"_data"
META_KEY = b"_meta"

logger = logging.getLogger("thredis")


class StreamMessage(BaseModel, Generic[T]):
    """Deserialized stream message passed to handlers. Generic over the body model type."""

    body: T
    headers: dict[str, str] = Field(default_factory=dict)
    msg_id: str
    stream: str


class RawMessage(BaseModel):
    """Raw Redis stream entry with ack capability. Internal to the consumer."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    msg_id: str
    stream: str
    group: str
    data: dict[bytes, bytes]
    headers: dict[str, str] = Field(default_factory=dict)
    redis: Redis = Field(exclude=True, repr=False)
    acked: bool = Field(default=False, exclude=True, repr=False)

    def deserialize(self, model_type: type[T]) -> T:
        raw = self.data.get(DATA_KEY)
        if raw is None:
            raise ValueError(
                f"Message {self.msg_id} has no '{DATA_KEY.decode()}' field. "
                f"Available fields: {list(self.data.keys())}"
            )
        return model_type.model_validate_json(raw)

    async def ack(self) -> None:
        if self.acked:
            return
        await self.redis.xack(self.stream, self.group, self.msg_id)
        self.acked = True


def extract_body_type(func: Any) -> type[BaseModel]:
    """Extract ``T`` from a handler's ``StreamMessage[T]`` type hint."""
    from typing import get_type_hints

    hints = get_type_hints(func)
    hints.pop("return", None)
    if not hints:
        raise TypeError(
            f"Handler '{func.__name__}' must have at least one "
            f"type-hinted parameter with StreamMessage[YourModel]"
        )

    hint = next(iter(hints.values()))

    if isinstance(hint, type) and issubclass(hint, StreamMessage):
        meta = getattr(hint, "__pydantic_generic_metadata__", None)
        if meta and meta.get("args"):
            body_type = meta["args"][0]
            if isinstance(body_type, type) and issubclass(body_type, BaseModel):
                return body_type
        raise TypeError(
            f"Handler '{func.__name__}': StreamMessage must be parameterized "
            f"with a Pydantic BaseModel, e.g. StreamMessage[YourModel]"
        )

    raise TypeError(
        f"Handler '{func.__name__}' parameter must be typed as "
        f"StreamMessage[YourModel], got {hint}"
    )


def serialize(
    model: BaseModel,
    headers: Optional[dict[str, str]] = None,
) -> dict[str, bytes]:
    fields: dict[str, bytes] = {
        DATA_KEY.decode(): model.model_dump_json().encode(),
    }
    if headers:
        fields[META_KEY.decode()] = json.dumps(headers).encode()
    return fields


def parse_headers(data: dict[bytes, bytes]) -> dict[str, str]:
    raw = data.get(META_KEY)
    if raw is None:
        return {}
    try:
        result: dict[str, str] = json.loads(raw)
        return result
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Failed to parse message headers, ignoring")
        return {}
