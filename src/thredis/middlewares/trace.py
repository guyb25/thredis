import logging
import time

from thredis.message import StreamMessage
from thredis.types import CallNext, MiddlewareFunc

logger = logging.getLogger("thredis")


def trace() -> MiddlewareFunc:
    """Log handler start/done with message ID and timing."""

    async def middleware(msg: StreamMessage, call_next: CallNext) -> None:
        body_type = type(msg.body).__name__
        start = time.monotonic()

        logger.info(f"[{msg.msg_id}] {body_type} on {msg.stream}")
        try:
            await call_next(msg)
            elapsed = time.monotonic() - start
            logger.info(f"[{msg.msg_id}] done ({elapsed:.2f}s)")
        except Exception:
            elapsed = time.monotonic() - start
            logger.error(f"[{msg.msg_id}] failed ({elapsed:.2f}s)")
            raise

    return middleware
