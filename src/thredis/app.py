from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable, Optional, Union

from pydantic import BaseModel
from redis.asyncio import Redis

from thredis._health import HealthServer
from thredis._shutdown import install_signal_handlers
from thredis.group import SubscriberGroup
from thredis.message import extract_body_type
from thredis.publisher import StreamPublisher
from thredis.subscriber import StreamSubscriber
from thredis.types import (
    UNSET,
    HandlerFunc,
    MiddlewareFunc,
    resolve_config,
)

logger = logging.getLogger("thredis")


class Thredis:
    """Async Redis Streams framework with concurrent processing and per-message ack."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        middlewares: Optional[list[MiddlewareFunc]] = None,
        shutdown_timeout: float = 30.0,
        log_level: Union[int, str] = logging.INFO,
        health_port: Optional[int] = None,
    ) -> None:
        logging.getLogger("thredis").setLevel(log_level)
        self._redis_url = redis_url
        self._middlewares = middlewares or []
        self._shutdown_timeout = shutdown_timeout
        self._health_port = health_port
        self._subscribers: list[StreamSubscriber] = []
        self._publisher: Optional[StreamPublisher] = None
        self._redis: Optional[Redis] = None
        self._health_server: Optional[HealthServer] = None

    def add_middleware(self, middleware: MiddlewareFunc) -> None:
        """Add a middleware to the app. Middlewares run in the order they're added."""
        self._middlewares.append(middleware)

    def subscriber(
        self,
        stream: str,
        group: str,
        concurrency: Optional[int] = None,
        batch_size: Optional[int] = None,
        claim_idle_after: Any = UNSET,
        max_retries: Any = UNSET,
        dead_letter_stream: Any = UNSET,
        block_timeout: Optional[int] = None,
        threaded: bool = False,
        maxlen: Any = UNSET,
    ) -> Callable:
        """Register a stream subscriber via decorator. Sync handlers must set ``threaded=True``."""

        def decorator(func: HandlerFunc) -> HandlerFunc:
            model_type = extract_body_type(func)

            if not inspect.iscoroutinefunction(func) and not threaded:
                raise TypeError(
                    f"Handler '{func.__name__}' is a sync function. "
                    f"Set threaded=True to run it in a thread, or make it async."
                )

            sub = StreamSubscriber(
                stream=stream,
                group=group,
                handler=func,
                model_type=model_type,
                concurrency=concurrency or 1,
                batch_size=batch_size or 10,
                claim_idle_after=resolve_config(claim_idle_after, None, None),
                max_retries=resolve_config(max_retries, None, None),
                dead_letter_stream=resolve_config(dead_letter_stream, None, None),
                block_timeout=block_timeout or 2000,
                threaded=threaded,
                maxlen=resolve_config(maxlen, None, None),
            )
            self._subscribers.append(sub)
            return func

        return decorator

    def include_group(self, group: SubscriberGroup) -> None:
        """Register all subscribers from a SubscriberGroup."""
        for kwargs in group._build_configs():
            sub = StreamSubscriber(**kwargs)
            self._subscribers.append(sub)

    async def publish(
        self,
        stream: str,
        model: BaseModel,
        headers: Optional[dict[str, str]] = None,
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> str:
        """Publish a Pydantic model to a stream. Returns the Redis message ID."""
        await self._ensure_publisher()
        publisher = self._publisher
        assert publisher is not None  # populated by _ensure_publisher
        return await publisher.publish(
            stream, model, headers=headers, maxlen=maxlen, approximate=approximate
        )

    async def status(self) -> dict:
        """Return health status of the app and all subscribers."""
        redis_ok = False
        if self._redis:
            try:
                await self._redis.ping()  # type: ignore[misc]
                redis_ok = True
            except Exception:
                pass

        subscriber_details = []
        for sub in self._subscribers:
            subscriber_details.append({
                "stream": sub.stream,
                "group": sub.group,
                "consumer": sub.consumer_name,
                "running": sub.active,
                "inflight": len(sub._tasks),
                "concurrency": sub.concurrency,
            })

        all_running = all(s.active for s in self._subscribers) if self._subscribers else False

        return {
            "healthy": redis_ok and all_running,
            "redis": redis_ok,
            "consumers": len(self._subscribers),
            "consumer_details": subscriber_details,
        }

    async def _ensure_publisher(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self._redis_url, decode_responses=False)
        if self._publisher is None:
            self._publisher = StreamPublisher(self._redis)

    def run(self) -> None:
        """Start the application (blocking). Handles SIGINT/SIGTERM for graceful shutdown."""
        asyncio.run(self._start())

    async def _start(self) -> None:
        shutdown_event = asyncio.Event()
        install_signal_handlers(shutdown_event)

        await self._ensure_publisher()

        for sub in self._subscribers:
            sub.middlewares = self._middlewares + sub.middlewares
            sub.app = self
            sub._chain = __import__("thredis.middleware", fromlist=["build_chain"]).build_chain(
                sub.handler, sub.middlewares, threaded=sub.threaded
            )
            await sub.start(self._redis_url)

        if not self._subscribers:
            logger.warning("No subscribers registered, nothing to do")
            return

        if self._health_port is not None:
            self._health_server = HealthServer(self._health_port, self.status)
            await self._health_server.start()

        self._log_startup_summary()

        await shutdown_event.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        """Stop all subscribers, health server, and close connections."""
        logger.info("Shutting down...")

        if self._health_server:
            await self._health_server.stop()
            self._health_server = None

        await asyncio.gather(
            *(s.stop(timeout=self._shutdown_timeout) for s in self._subscribers),
            return_exceptions=True,
        )
        self._subscribers.clear()

        if self._redis:
            await self._redis.aclose()
            self._redis = None
            self._publisher = None

        logger.info("thredis stopped")

    def _log_startup_summary(self) -> None:
        groups: dict[str, list[StreamSubscriber]] = {}
        for sub in self._subscribers:
            groups.setdefault(sub.group, []).append(sub)

        logger.info(f"thredis started ({len(self._subscribers)} subscribers)")

        for group_name, members in groups.items():
            logger.info(f"  {group_name}")
            for sub in members:
                flags = [f"concurrency={sub.concurrency}"]
                if sub.threaded:
                    flags.append("threaded")
                if sub.claim_idle_after is not None:
                    flags.append(f"autoclaim={sub.claim_idle_after}ms")
                if sub.max_retries is not None:
                    flags.append(f"max_retries={sub.max_retries}")
                if sub.dead_letter_stream:
                    flags.append(f"dlq={sub.dead_letter_stream}")
                if sub.maxlen is not None:
                    flags.append(f"maxlen={sub.maxlen}")
                group_mw_count = len(sub.middlewares) - len(self._middlewares)
                if group_mw_count > 0:
                    flags.append(f"{group_mw_count} group mw")
                logger.info(f"    {sub.stream} -> {sub.handler.__name__}  [{', '.join(flags)}]")
