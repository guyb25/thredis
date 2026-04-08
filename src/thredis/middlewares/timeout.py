import asyncio
import logging
from typing import Optional

from thredis.message import StreamMessage
from thredis.types import CallNext, MiddlewareFunc

logger = logging.getLogger("thredis")


def timeout(seconds: float) -> MiddlewareFunc:
    """Cancel the handler if it exceeds the given duration.

    The message is NOT acked on timeout, so it stays in the PEL
    and can be autoclaimed later.
    """

    async def middleware(msg: StreamMessage, call_next: CallNext) -> None:
        try:
            await asyncio.wait_for(call_next(msg), timeout=seconds)
        except asyncio.TimeoutError:
            logger.warning(f"Handler timed out after {seconds}s for message {msg.msg_id}")
            raise

    return middleware
