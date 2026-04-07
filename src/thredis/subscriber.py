from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from typing import Any, Optional

from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from thredis._maintenance import ConsumerMaintenance
from thredis.context import set_current_app
from thredis.message import RawMessage, StreamMessage, parse_headers
from thredis.middleware import build_chain
from thredis.types import HandlerFunc, MiddlewareFunc

logger = logging.getLogger("thredis")


def _generate_consumer_name(group: str) -> str:
    host = socket.gethostname()
    pid = os.getpid()
    uid = uuid.uuid4().hex[:8]
    return f"{group}-{host}-{pid}-{uid}"


class StreamSubscriber:
    """Concurrent Redis stream subscriber with per-message ack and autoclaim."""

    def __init__(
        self,
        *,
        stream: str,
        group: str,
        handler: HandlerFunc,
        model_type: type[BaseModel],
        concurrency: int = 1,
        batch_size: int = 10,
        claim_idle_after: Optional[int] = None,
        max_retries: Optional[int] = None,
        dead_letter_stream: Optional[str] = None,
        block_timeout: int = 2000,
        threaded: bool = False,
        maxlen: Optional[int] = None,
        middlewares: Optional[list[MiddlewareFunc]] = None,
        app: Any = None,
    ) -> None:
        self.stream = stream
        self.group = group
        self.handler = handler
        self.model_type = model_type
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.claim_idle_after = claim_idle_after
        self.max_retries = max_retries
        self.dead_letter_stream = dead_letter_stream
        self.block_timeout = block_timeout
        self.threaded = threaded
        self.maxlen = maxlen
        self.middlewares = middlewares or []
        self.app = app

        self._consumer_name = _generate_consumer_name(group)
        self._chain = build_chain(handler, self.middlewares, threaded=threaded)
        self._maintenance = ConsumerMaintenance(self, self._consumer_name)
        self._active = False
        self._tasks: set[asyncio.Task] = set()
        self._sem: Optional[asyncio.Semaphore] = None
        self._redis: Optional[Redis] = None
        self._loop_task: Optional[asyncio.Task] = None

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    @property
    def active(self) -> bool:
        return self._active

    async def start(self, redis_url: str) -> None:
        self._redis = Redis.from_url(redis_url, decode_responses=False)
        self._sem = asyncio.Semaphore(self.concurrency)
        self._active = True

        try:
            await self._redis.xgroup_create(
                name=self.stream,
                groupname=self.group,
                id="0",
                mkstream=True,
            )
            logger.debug(f"Created consumer group '{self.group}' on stream '{self.stream}'")
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        self._loop_task = asyncio.create_task(
            self._run(), name=f"thredis-{self.stream}-{self._consumer_name}"
        )

    async def stop(self, timeout: float = 30.0) -> None:
        self._active = False

        if self._loop_task and not self._loop_task.done():
            try:
                await asyncio.wait_for(self._loop_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._loop_task.cancel()

        if self._tasks:
            logger.info(f"Waiting for {len(self._tasks)} in-flight messages (timeout={timeout}s)...")
            done, pending = await asyncio.wait(self._tasks, timeout=timeout)
            if pending:
                logger.warning(f"Cancelling {len(pending)} tasks that didn't finish in time")
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        if self._redis:
            try:
                await self._redis.xgroup_delconsumer(
                    name=self.stream,
                    groupname=self.group,
                    consumername=self._consumer_name,
                )
            except Exception:
                pass
            await self._redis.aclose()

        logger.info(f"Subscriber '{self._consumer_name}' stopped")

    async def _run(self) -> None:
        redis = self._redis
        sem = self._sem
        if redis is None or sem is None:
            return

        while self._active:
            try:
                await sem.acquire()
                sem.release()

                if not self._active:
                    break

                result = await redis.xreadgroup(
                    groupname=self.group,
                    consumername=self._consumer_name,
                    streams={self.stream: ">"},
                    count=self.batch_size,
                    block=self.block_timeout,
                )

                if not result:
                    await self._maintenance.try_autoclaim(redis, self._dispatch)
                    await self._maintenance.cleanup_dead_consumers(redis)
                    continue

                for _stream_name, messages in result:
                    for msg_id, fields in messages:
                        await self._dispatch(msg_id, fields)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in subscriber loop, retrying in 1s...")
                await asyncio.sleep(1.0)

    async def _dispatch(self, msg_id: bytes, fields: dict[bytes, bytes]) -> None:
        sem = self._sem
        if sem is None:
            return

        await sem.acquire()
        task = asyncio.create_task(
            self._process_one(msg_id, fields),
            name=f"thredis-msg-{msg_id!r}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if self._sem is not None:
            self._sem.release()

        if not task.cancelled() and task.exception():
            logger.error(f"Unexpected error in message task: {task.exception()}")

    async def _process_one(
        self, raw_id: bytes, fields: dict[bytes, bytes]
    ) -> None:
        redis = self._redis
        if redis is None:
            return

        msg_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        raw_msg = RawMessage(
            msg_id=msg_id,
            stream=self.stream,
            group=self.group,
            data=fields,
            headers=parse_headers(fields),
            redis=redis,
        )

        try:
            if self.app is not None:
                set_current_app(self.app)
            model = raw_msg.deserialize(self.model_type)
            stream_msg = StreamMessage(
                body=model,
                headers=raw_msg.headers,
                msg_id=raw_msg.msg_id,
                stream=raw_msg.stream,
            )
            await self._chain(stream_msg, raw_msg)
            await raw_msg.ack()
            await self._maintenance.trim_if_needed(redis)
        except Exception:
            logger.exception(f"Handler failed for message {msg_id} on '{self.stream}', leaving in PEL")
