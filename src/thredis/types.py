from __future__ import annotations

from typing import Any, Awaitable, Callable, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

HandlerFunc = Callable[..., Any]
CallNext = Callable[..., Awaitable[None]]
MiddlewareFunc = Callable[..., Awaitable[None]]

UNSET = object()  # sentinel distinct from None


def resolve_config(explicit: Any, default: Any, fallback: Any) -> Any:
    """Resolve a config value with precedence: explicit > default > fallback."""
    if explicit is not UNSET and explicit is not None:
        return explicit
    if default is not None:
        return default
    return fallback
