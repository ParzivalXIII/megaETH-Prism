"""Redis Stream consumer base class — XREADGROUP, XACK, XAUTOCLAIM, dead-letter.

StreamConsumer
==============
Provides async iteration over a Redis consumer group with full lifecycle
management: consume (XREADGROUP), acknowledge (XACK), recovery
(XAUTOCLAIM), and poison-message routing (XADD to dead-letter stream).

Usage::

    consumer = StreamConsumer("megaeth:workers:postgres", "worker-1")
    async for entry_id, data in consumer.consume():
        try:
            entry = StreamConsumer.parse_entry(data)
            mini_block = StreamConsumer.parse_mini_block(entry)
            # ... process ...
            await consumer.ack(entry_id)
        except Exception:
            await consumer.dead_letter(entry_id, data, reason="parse error")
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as redis

from app.core.config import settings
from app.core.logging import get_logger
from app.schemas import (
    MiniBlockPayload,
    STREAM_FIELD_BLOCK_NUMBER,
    STREAM_FIELD_CORRELATION_ID,
    STREAM_FIELD_INGESTED_AT,
    STREAM_FIELD_RAW_PAYLOAD,
    StreamEntry,
)

logger = get_logger("megaeth.streams.consumer")


class StreamConsumer:
    """Redis Stream consumer group subscriber.

    Parameters
    ----------
    group_name:
        Consumer group name (e.g. ``"megaeth:workers:postgres"``).
    consumer_name:
        Unique consumer identifier within the group (e.g. ``"worker-1"``).
    redis_client:
        Optional pre-connected Redis client.  If omitted a lazy connection
        is created from ``settings.redis_url``.
    stream_name:
        Override stream name (defaults to ``settings.stream_name``).
    dead_letter_stream:
        Override dead-letter stream (defaults to
        ``settings.dead_letter_stream``).
    block_ms:
        How long XREADGROUP blocks waiting for new messages (ms).
    """

    def __init__(
        self,
        group_name: str,
        consumer_name: str,
        redis_client: redis.Redis | None = None,
        stream_name: str | None = None,
        dead_letter_stream: str | None = None,
        block_ms: int = 1000,
    ) -> None:
        self.group_name = group_name
        self.consumer_name = consumer_name
        self._redis = redis_client
        self._owned_redis = redis_client is None
        self.stream_name = stream_name or settings.stream_name
        self.dead_letter_stream = (
            dead_letter_stream or settings.dead_letter_stream
        )
        self.block_ms = block_ms
        self._shutdown_event: asyncio.Event | None = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.from_url(
                settings.redis_url, decode_responses=True
            )
            await self._redis.ping()
        return self._redis

    # ------------------------------------------------------------------
    # Consume (XREADGROUP)
    # ------------------------------------------------------------------

    async def consume(self) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Async generator yielding ``(stream_id, data)`` tuples.

        Uses ``XREADGROUP`` with ``BLOCK`` to wait for new messages.
        Yields raw data dicts with string values.

        The generator loops forever until ``shutdown()`` is called.
        """
        r = await self._get_redis()

        while True:
            if self._shutdown_event is not None and self._shutdown_event.is_set():
                break

            try:
                results = await r.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=10,
                    block=self.block_ms,
                )
            except redis.ResponseError as e:
                if "NOGROUP" in str(e):
                    logger.warning(
                        "consumer group not found, creating",
                        group=self.group_name,
                    )
                    try:
                        await r.xgroup_create(
                            self.stream_name,
                            self.group_name,
                            id="$",
                            mkstream=True,
                        )
                    except redis.ResponseError:
                        pass
                    continue
                raise

            if not results:
                continue

            for _stream_name, entries in results:
                for entry_id, data in entries:
                    yield (entry_id, data)

    # ------------------------------------------------------------------
    # Acknowledge (XACK)
    # ------------------------------------------------------------------

    async def ack(self, entry_id: str) -> None:
        """Acknowledge a message as processed.

        Removes the message from the Pending Entries List (PEL).
        Call this *after* the message has been durably processed.
        """
        r = await self._get_redis()
        await r.xack(self.stream_name, self.group_name, entry_id)

    # ------------------------------------------------------------------
    # Recovery (XAUTOCLAIM)
    # ------------------------------------------------------------------

    async def claim_pending(
        self, min_idle_ms: int = 30000
    ) -> list[tuple[str, dict[str, Any]]]:
        """Reclaim orphaned messages from crashed consumers.

        Uses ``XAUTOCLAIM`` to transfer messages idle longer than
        *min_idle_ms* to this consumer.

        Returns a list of ``(entry_id, data)`` tuples for the reclaimed
        messages.  The caller should process and then XACK each one.
        """
        r = await self._get_redis()
        claimed: list[tuple[str, dict[str, Any]]] = []

        try:
            result = await r.xautoclaim(
                name=self.stream_name,
                groupname=self.group_name,
                consumername=self.consumer_name,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=100,
            )
            # xautoclaim returns (next_start_id, [entries])
            entries = result[1] if len(result) > 1 else []
            for entry_id, data in entries:
                claimed.append((entry_id, data))
        except redis.ResponseError as e:
            logger.warning("XAUTOCLAIM failed", error=str(e))

        return claimed

    # ------------------------------------------------------------------
    # Dead-letter routing
    # ------------------------------------------------------------------

    async def dead_letter(
        self,
        entry_id: str,
        data: dict[str, Any],
        reason: str = "",
    ) -> None:
        """Move a poison-pill message to the dead-letter stream.

        The original stream name, entry ID, consumer group, consumer name,
        and failure reason are included in the dead-letter entry.

        After calling this, the caller **must** XACK the original entry to
        remove it from the PEL (otherwise it will be re-claimed and
        re-dead-lettered infinitely).
        """
        r = await self._get_redis()

        dead_data: dict[str, Any] = {
            "original_stream": self.stream_name,
            "original_entry_id": entry_id,
            "consumer_group": self.group_name,
            "consumer": self.consumer_name,
            "data": json.dumps(data),
            "reason": reason,
        }

        await r.xadd(self.dead_letter_stream, dead_data)  # type: ignore[arg-type]
        logger.warning(
            "message sent to dead letter",
            entry_id=entry_id,
            stream=self.dead_letter_stream,
            reason=reason,
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def parse_entry(data: dict[str, Any]) -> StreamEntry:
        """Decode raw Redis Stream entry data into a typed :class:`StreamEntry`.

        Redis Stream ``XREADGROUP`` returns all field values as strings.
        This method casts them to the correct Python types.
        """
        return StreamEntry(
            block_number=int(data.get(STREAM_FIELD_BLOCK_NUMBER, 0)),
            raw_payload=data.get(STREAM_FIELD_RAW_PAYLOAD, ""),
            ingested_at=float(data.get(STREAM_FIELD_INGESTED_AT, 0)),
            correlation_id=data.get(STREAM_FIELD_CORRELATION_ID, ""),
        )

    @staticmethod
    def parse_mini_block(entry: StreamEntry) -> MiniBlockPayload:
        """Deserialise a :class:`StreamEntry`'s ``raw_payload`` JSON into a
        :class:`MiniBlockPayload`."""
        payload = json.loads(entry.raw_payload)
        return MiniBlockPayload.model_validate(payload)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the Redis connection if we own it."""
        if self._owned_redis and self._redis is not None:
            await self._redis.close()
            self._redis = None
