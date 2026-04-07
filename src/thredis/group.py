from __future__ import annotations

import inspect
from typing import Any, Callable, Optional

from thredis.message import extract_body_type
from thredis.types import (
    UNSET,
    HandlerFunc,
    MiddlewareFunc,
    resolve_config,
)


class SubscriberGroup:
    """Group of subscribers with shared defaults and middleware.

    Per-subscriber values override group defaults.
    Group middleware runs after app-level middleware, before the handler.
    """

    def __init__(
        self,
        group: Optional[str] = None,
        concurrency: Optional[int] = None,
        batch_size: Optional[int] = None,
        claim_idle_after: Optional[int] = None,
        max_retries: Optional[int] = None,
        dead_letter_stream: Optional[str] = None,
        block_timeout: Optional[int] = None,
        threaded: Optional[bool] = None,
        maxlen: Optional[int] = None,
        middlewares: Optional[list[MiddlewareFunc]] = None,
    ) -> None:
        self._group = group
        self._concurrency = concurrency
        self._batch_size = batch_size
        self._claim_idle_after = claim_idle_after
        self._max_retries = max_retries
        self._dead_letter_stream = dead_letter_stream
        self._block_timeout = block_timeout
        self._threaded = threaded
        self._maxlen = maxlen
        self._middlewares = middlewares or []
        self._pending: list[tuple[dict, HandlerFunc]] = []

    def add_middleware(self, middleware: MiddlewareFunc) -> None:
        """Add middleware to this group. Runs after app-level middleware."""
        self._middlewares.append(middleware)

    def subscriber(
        self,
        stream: str,
        group: Any = UNSET,
        concurrency: Optional[int] = None,
        batch_size: Optional[int] = None,
        claim_idle_after: Any = UNSET,
        max_retries: Any = UNSET,
        dead_letter_stream: Any = UNSET,
        block_timeout: Optional[int] = None,
        threaded: Optional[bool] = None,
        maxlen: Any = UNSET,
    ) -> Callable:
        """Register a subscriber in this group. Per-subscriber values override group defaults."""

        def decorator(func: HandlerFunc) -> HandlerFunc:
            self._pending.append((
                {
                    "stream": stream,
                    "group": group,
                    "concurrency": concurrency,
                    "batch_size": batch_size,
                    "claim_idle_after": claim_idle_after,
                    "max_retries": max_retries,
                    "dead_letter_stream": dead_letter_stream,
                    "block_timeout": block_timeout,
                    "threaded": threaded,
                    "maxlen": maxlen,
                },
                func,
            ))
            return func

        return decorator

    def _build_configs(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for kwargs, func in self._pending:
            model_type = extract_body_type(func)

            group = kwargs["group"] if kwargs["group"] is not UNSET else self._group
            if group is None:
                raise ValueError(
                    f"Subscriber '{func.__name__}' has no group. "
                    f"Set it on the SubscriberGroup or on the @subscriber() decorator."
                )

            threaded = kwargs["threaded"]
            if threaded is None:
                threaded = self._threaded if self._threaded is not None else False

            if not inspect.iscoroutinefunction(func) and not threaded:
                raise TypeError(
                    f"Handler '{func.__name__}' is a sync function. "
                    f"Set threaded=True on the group or subscriber, or make it async."
                )

            results.append({
                "stream": kwargs["stream"],
                "group": group,
                "handler": func,
                "model_type": model_type,
                "concurrency": resolve_config(kwargs["concurrency"], self._concurrency, 1),
                "batch_size": resolve_config(kwargs["batch_size"], self._batch_size, 10),
                "claim_idle_after": resolve_config(kwargs["claim_idle_after"], self._claim_idle_after, None),
                "max_retries": resolve_config(kwargs["max_retries"], self._max_retries, None),
                "dead_letter_stream": resolve_config(kwargs["dead_letter_stream"], self._dead_letter_stream, None),
                "block_timeout": resolve_config(kwargs["block_timeout"], self._block_timeout, 2000),
                "threaded": threaded,
                "maxlen": resolve_config(kwargs["maxlen"], self._maxlen, None),
                "middlewares": self._middlewares,
            })

        return results
