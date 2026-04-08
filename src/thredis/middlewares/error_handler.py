import logging
from typing import Any, Awaitable, Callable, Union

from thredis.message import StreamMessage
from thredis.types import CallNext, MiddlewareFunc

logger = logging.getLogger("thredis")

ErrorCallback = Union[
    Callable[[StreamMessage, Exception], None],
    Callable[[StreamMessage, Exception], Awaitable[None]],
]


def error_handler(on_error: ErrorCallback) -> MiddlewareFunc:
    """Call a user function when a handler raises. Re-raises after calling.

    The callback receives the message and the exception. It can be sync or async.
    The message is NOT acked - the error still propagates to thredis for PEL handling.
    """
    import inspect
    _is_async = inspect.iscoroutinefunction(on_error)

    async def middleware(msg: StreamMessage, call_next: CallNext) -> None:
        try:
            await call_next(msg)
        except Exception as e:
            try:
                if _is_async:
                    await on_error(msg, e)  # type: ignore[misc]
                else:
                    on_error(msg, e)  # type: ignore[misc]
            except Exception:
                logger.exception("Error in error_handler callback")
            raise

    return middleware
