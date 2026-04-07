from __future__ import annotations

import asyncio
import inspect
from typing import Awaitable, Callable

from thredis.message import RawMessage, StreamMessage
from thredis.types import HandlerFunc, MiddlewareFunc

ChainFunc = Callable[[StreamMessage, RawMessage], Awaitable[None]]


def build_chain(
    handler: HandlerFunc,
    middlewares: list[MiddlewareFunc],
    threaded: bool = False,
) -> ChainFunc:
    """Compose middlewares and handler into a single callable chain."""
    is_async = inspect.iscoroutinefunction(handler)

    async def invoke(msg: StreamMessage, raw: RawMessage) -> None:
        if is_async:
            await handler(msg)
        else:
            handler(msg)

    chain: ChainFunc = invoke
    for mw in reversed(middlewares):

        def _wrap(current_chain: ChainFunc, middleware: MiddlewareFunc) -> ChainFunc:
            async def wrapped(msg: StreamMessage, raw: RawMessage) -> None:
                async def call_next(m: StreamMessage) -> None:
                    await current_chain(m, raw)

                await middleware(msg, call_next)

            return wrapped

        chain = _wrap(chain, mw)

    if threaded:
        inner_chain = chain

        async def threaded_chain(msg: StreamMessage, raw: RawMessage) -> None:
            def _run_in_thread() -> None:
                coro = inner_chain(msg, raw)
                asyncio.run(coro)  # type: ignore[arg-type]

            await asyncio.to_thread(_run_in_thread)

        return threaded_chain

    return chain
