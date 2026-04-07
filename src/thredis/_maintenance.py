"""Consumer maintenance: autoclaim, dead letter, trimming, stale consumer cleanup."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Awaitable, Callable, Optional, Protocol, runtime_checkable

from redis.asyncio import Redis

if TYPE_CHECKING:
    pass

logger = logging.getLogger("thredis")


@runtime_checkable
class _SubscriberLike(Protocol):
    stream: str
    group: str
    batch_size: int
    claim_idle_after: Optional[int]
    max_retries: Optional[int]
    dead_letter_stream: Optional[str]
    maxlen: Optional[int]


class ConsumerMaintenance:
    """Periodic maintenance for a stream subscriber: autoclaim, dead letter, trimming."""

    def __init__(
        self,
        sub: _SubscriberLike,
        consumer_name: str,
        autoclaim_interval: float = 10.0,
        cleanup_interval: float = 60.0,
    ) -> None:
        self._sub = sub
        self._consumer_name = consumer_name
        self._autoclaim_interval = autoclaim_interval
        self._cleanup_interval = cleanup_interval
        self._autoclaim_cursor = b"0-0"
        self._last_claim_time = 0.0
        self._ack_count_since_trim = 0
        self._last_trim_time = 0.0
        self._last_cleanup_time = 0.0

    async def try_autoclaim(
        self, redis: Redis, dispatch_fn: Callable[..., Awaitable[None]]
    ) -> None:
        if self._sub.claim_idle_after is None:
            return

        now = time.monotonic()
        if now - self._last_claim_time < self._autoclaim_interval:
            return
        self._last_claim_time = now

        try:
            result = await redis.xautoclaim(
                name=self._sub.stream,
                groupname=self._sub.group,
                consumername=self._consumer_name,
                min_idle_time=self._sub.claim_idle_after,
                start_id=self._autoclaim_cursor,
                count=self._sub.batch_size,
            )

            next_cursor, claimed, _deleted = result
            self._autoclaim_cursor = next_cursor

            if next_cursor == b"0-0":
                self._autoclaim_cursor = b"0-0"

            if not claimed:
                return

            if self._sub.max_retries is not None:
                claimed = await self._filter_dead_messages(redis, claimed)

            if claimed:
                logger.info(f"Autoclaimed {len(claimed)} messages on '{self._sub.stream}'")
                for msg_id, fields in claimed:
                    await dispatch_fn(msg_id, fields)

        except Exception:
            logger.exception(f"Autoclaim failed on '{self._sub.stream}'")

    async def _filter_dead_messages(
        self, redis: Redis, claimed: list[tuple[bytes, dict[bytes, bytes]]]
    ) -> list[tuple[bytes, dict[bytes, bytes]]]:
        max_retries = self._sub.max_retries
        if max_retries is None:
            return claimed

        claimed_ids = [msg_id for msg_id, _ in claimed]
        if not claimed_ids:
            return claimed

        pending_details = await redis.xpending_range(
            name=self._sub.stream,
            groupname=self._sub.group,
            min=claimed_ids[0],
            max=claimed_ids[-1],
            count=len(claimed_ids),
            consumername=self._consumer_name,
        )

        delivery_counts: dict[bytes, int] = {}
        for detail in pending_details:
            delivery_counts[detail["message_id"]] = detail["times_delivered"]

        alive = []
        for msg_id, fields in claimed:
            count = delivery_counts.get(msg_id, 1)
            if count > max_retries:
                await self._handle_dead_message(redis, msg_id, fields, count)
            else:
                alive.append((msg_id, fields))

        return alive

    async def _handle_dead_message(
        self,
        redis: Redis,
        msg_id: bytes,
        fields: dict[bytes, bytes],
        delivery_count: int,
    ) -> None:
        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)

        if self._sub.dead_letter_stream:
            dead_fields = dict(fields)
            dead_fields[b"_dead_source_stream"] = self._sub.stream.encode()
            dead_fields[b"_dead_source_id"] = msg_id
            dead_fields[b"_dead_delivery_count"] = str(delivery_count).encode()
            dead_fields[b"_dead_group"] = self._sub.group.encode()

            await redis.xadd(
                name=self._sub.dead_letter_stream,
                fields=dead_fields,  # type: ignore[arg-type]
            )
            logger.warning(f"Message {msg_id_str} moved to dead letter stream '{self._sub.dead_letter_stream}' after {delivery_count} deliveries")
        else:
            logger.warning(f"Message {msg_id_str} discarded after {delivery_count} deliveries (no dead letter stream configured)")

        await redis.xack(self._sub.stream, self._sub.group, msg_id)

    async def trim_if_needed(self, redis: Redis) -> None:
        if self._sub.maxlen is None:
            return

        self._ack_count_since_trim += 1
        now = time.monotonic()

        if self._ack_count_since_trim < 100 and now - self._last_trim_time < 10.0:
            return

        self._ack_count_since_trim = 0
        self._last_trim_time = now

        try:
            await redis.xtrim(
                name=self._sub.stream,
                maxlen=self._sub.maxlen,
                approximate=True,
            )
        except Exception:
            logger.exception(f"Stream trim failed on '{self._sub.stream}'")

    async def cleanup_dead_consumers(self, redis: Redis) -> None:
        if self._sub.claim_idle_after is None:
            return

        now = time.monotonic()
        if now - self._last_cleanup_time < self._cleanup_interval:
            return
        self._last_cleanup_time = now

        idle_threshold = self._sub.claim_idle_after * 2

        try:
            consumers = await redis.xinfo_consumers(
                name=self._sub.stream,
                groupname=self._sub.group,
            )

            for consumer_info in consumers:
                name = consumer_info.get("name", b"")
                if isinstance(name, bytes):
                    name = name.decode()
                idle = consumer_info.get("idle", 0)
                pending = consumer_info.get("pending", 0)

                if name == self._consumer_name:
                    continue

                if idle > idle_threshold and pending == 0:
                    await redis.xgroup_delconsumer(
                        name=self._sub.stream,
                        groupname=self._sub.group,
                        consumername=name,
                    )
                    logger.info(f"Cleaned up dead consumer '{name}' on '{self._sub.stream}' (idle {idle}ms)")
        except Exception:
            logger.exception(f"Consumer cleanup failed on '{self._sub.stream}'")
