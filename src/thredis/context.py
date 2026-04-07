from __future__ import annotations

import threading
from contextvars import ContextVar
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel
from redis.asyncio import Redis

from thredis.message import serialize

if TYPE_CHECKING:
    from thredis.app import Thredis

_current_app: ContextVar[Thredis] = ContextVar("_current_app")
_main_thread_id: ContextVar[int] = ContextVar("_main_thread_id")


def set_current_app(app: Thredis) -> None:
    _current_app.set(app)
    thread_id = threading.current_thread().ident
    if thread_id is not None:
        _main_thread_id.set(thread_id)


def _is_in_worker_thread() -> bool:
    try:
        main_id = _main_thread_id.get()
        return threading.current_thread().ident != main_id
    except LookupError:
        return False


async def publish(
    stream: str,
    model: BaseModel,
    headers: Optional[dict[str, str]] = None,
    maxlen: Optional[int] = None,
    approximate: bool = True,
) -> str:
    """Publish from inside a handler without a reference to the app.

    Thread-safe: creates a temporary Redis connection for threaded handlers.
    """
    try:
        app = _current_app.get()
    except LookupError:
        raise RuntimeError(
            "publish() called outside of a thredis handler context. "
            "Use app.publish() directly, or ensure you're calling from "
            "within a subscriber handler."
        )

    if _is_in_worker_thread():
        redis = Redis.from_url(app._redis_url, decode_responses=False)
        try:
            fields = serialize(model, headers=headers)
            msg_id: bytes = await redis.xadd(
                name=stream,
                fields=fields,  # type: ignore[arg-type]
                maxlen=maxlen,
                approximate=approximate and maxlen is not None,
            )
            return msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
        finally:
            await redis.aclose()
    else:
        return await app.publish(
            stream, model, headers=headers, maxlen=maxlen, approximate=approximate
        )
