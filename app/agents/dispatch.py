"""ExecutionQueue — Redis STREAM-backed execution queue for agent intents.

Follows the Phase 1 ``StreamBuffer`` + ``StreamConsumer`` pattern exactly:
- XADD for enqueue
- XREADGROUP with consumer groups for at-least-once delivery
- XACK for acknowledgement
- XAUTOCLAIM for orphan recovery
- Dedicated dead-letter stream for poison pills

Key design decisions:
- Uses Redis STREAM (not LIST) for persistent, replayable ordered delivery.
- Consumer group pattern matches Phase 1 ingestion pipeline.
- Execution results stored in Redis HASH with 24h TTL.
- All Redis keys prefixed with ``megaeth:``.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as redis

from app.core.config import settings
from app.core.logging import get_logger
from app.schemas.intent import ExecutionPayload

logger = get_logger("megaeth.agents.dispatch")


class ExecutionQueue:
    """Redis STREAM-backed queue for agent execution plans.

    Agents produce ``ExecutionPayload`` JSON and call ``enqueue()``.
    Workers call ``consume()`` to receive new messages, ``ack()`` after
    successful processing, and ``dead_letter()`` for poison pills.
    """

    STREAM_KEY: str = "megaeth:stream:executions"
    GROUP_NAME: str = "megaeth:workers:executors"
    DEAD_LETTER: str = "megaeth:stream:executions:dead"
    RESULT_PREFIX: str = "megaeth:executions:"
    MAXLEN: int = 100_000

    def __init__(self, redis_client: redis.Redis | None = None) -> None:
        self._redis: redis.Redis | None = redis_client
        self._owned_redis = redis_client is None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def _get_redis(self) -> redis.Redis:
        """Lazy Redis connection."""
        if self._redis is None:
            self._redis = redis.from_url(  # type: ignore[no-untyped-call]
                settings.redis_url, decode_responses=True
            )
            await self._redis.ping()
        return self._redis

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Ensure the execution stream, dead-letter stream, and consumer group exist.

        Idempotent — safe to call on every worker restart.
        """
        r = await self._get_redis()

        # Create execution stream with init marker
        if not await r.exists(self.STREAM_KEY):
            init_id = await r.xadd(self.STREAM_KEY, {"init": "1"}, maxlen=self.MAXLEN)
            await r.xdel(self.STREAM_KEY, init_id)
            logger.info("execution stream created", stream=self.STREAM_KEY)
        else:
            await r.xtrim(self.STREAM_KEY, maxlen=self.MAXLEN, approximate=True)

        # Create dead-letter stream
        if not await r.exists(self.DEAD_LETTER):
            init_id = await r.xadd(self.DEAD_LETTER, {"init": "1"})
            await r.xdel(self.DEAD_LETTER, init_id)
            logger.info("dead-letter stream created", stream=self.DEAD_LETTER)

        # Create consumer group
        try:
            await r.xgroup_create(
                self.STREAM_KEY, self.GROUP_NAME, id="$", mkstream=True
            )
            logger.info("consumer group created", group=self.GROUP_NAME)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    # ------------------------------------------------------------------
    # Enqueue (XADD)
    # ------------------------------------------------------------------

    async def enqueue(self, payload: ExecutionPayload) -> str:
        """Write an execution plan to the stream.

        Returns the Redis stream entry ID (e.g. ``"1234567890123-0"``).
        """
        r = await self._get_redis()
        entry_id = await r.xadd(
            self.STREAM_KEY,
            {
                "payload": payload.model_dump_json(),
                "created_at": str(payload.created_at),
                "created_by": payload.created_by_agent,
            },
            maxlen=self.MAXLEN,
            approximate=True,
        )
        return str(entry_id) if entry_id is not None else ""

    # ------------------------------------------------------------------
    # Consume (XREADGROUP)
    # ------------------------------------------------------------------

    async def consume(
        self, block_ms: int = 5000
    ) -> AsyncIterator[tuple[str, ExecutionPayload]]:
        """Async generator yielding ``(entry_id, ExecutionPayload)``.

        Uses ``XREADGROUP`` with ``>`` to receive only new messages.
        On ``NOGROUP`` error, auto-creates the consumer group at ``$``.

        **Poison-pill handling**: entries with empty payloads or unparseable
        payloads are routed to the dead-letter stream and acknowledged
        immediately, preventing PEL accumulation.
        """
        r = await self._get_redis()

        while True:
            try:
                results = await r.xreadgroup(
                    groupname=self.GROUP_NAME,
                    consumername="executor-1",
                    streams={self.STREAM_KEY: ">"},
                    count=5,
                    block=block_ms,
                )
            except redis.ResponseError as e:
                if "NOGROUP" in str(e):
                    logger.warning(
                        "consumer group not found, creating",
                        group=self.GROUP_NAME,
                    )
                    try:
                        await r.xgroup_create(
                            self.STREAM_KEY, self.GROUP_NAME, id="$", mkstream=True
                        )
                    except redis.ResponseError:
                        pass
                    continue
                raise

            if not results:
                continue

            for _stream_name, entries in results:
                for entry_id, data in entries:
                    payload_json = data.get("payload", "")
                    if not payload_json:
                        logger.warning(
                            "dead-lettering entry with empty payload",
                            entry_id=entry_id,
                        )
                        await self._dead_letter_raw(
                            r, entry_id, data, reason="empty payload"
                        )
                        await self.ack(entry_id)
                        continue
                    try:
                        payload = ExecutionPayload.model_validate_json(payload_json)
                    except Exception as e:
                        logger.error(
                            "dead-lettering entry with unparseable payload",
                            entry_id=entry_id,
                            error=str(e),
                        )
                        await self._dead_letter_raw(
                            r, entry_id, data, reason=f"parse error: {e}"
                        )
                        await self.ack(entry_id)
                        continue
                    yield (entry_id, payload)

    # ------------------------------------------------------------------
    # Acknowledge (XACK)
    # ------------------------------------------------------------------

    async def ack(self, entry_id: str) -> None:
        """Acknowledge a message as processed.

        Removes the message from the Pending Entries List (PEL).
        Call *after* the message has been durably processed.
        """
        r = await self._get_redis()
        await r.xack(self.STREAM_KEY, self.GROUP_NAME, entry_id)

    # ------------------------------------------------------------------
    # Orphan recovery (XAUTOCLAIM)
    # ------------------------------------------------------------------

    async def claim_pending(
        self, min_idle_ms: int = 30000
    ) -> list[tuple[str, ExecutionPayload]]:
        """Reclaim orphaned messages from crashed consumers.

        Returns a list of ``(entry_id, ExecutionPayload)`` tuples.
        """
        r = await self._get_redis()
        claimed: list[tuple[str, ExecutionPayload]] = []

        try:
            result = await r.xautoclaim(
                name=self.STREAM_KEY,
                groupname=self.GROUP_NAME,
                consumername="executor-1",
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=50,
            )
            # xautoclaim returns (next_start_id, [entries])
            entries = result[1] if len(result) > 1 else []
            for entry_id, data in entries:
                payload_json = data.get("payload", "")
                if payload_json:
                    try:
                        payload = ExecutionPayload.model_validate_json(payload_json)
                        claimed.append((entry_id, payload))
                    except Exception as e:
                        logger.warning(
                            "failed to parse claimed payload",
                            entry_id=entry_id,
                            error=str(e),
                        )
        except redis.ResponseError as e:
            logger.warning("XAUTOCLAIM failed", error=str(e))

        return claimed

    # ------------------------------------------------------------------
    # Dead-letter routing
    # ------------------------------------------------------------------

    async def _dead_letter_raw(
        self,
        r: redis.Redis,
        entry_id: str,
        data: dict[str, str],
        reason: str = "",
    ) -> None:
        """Move a raw (unparseable) entry to the dead-letter stream.

        This is a low-level variant used internally by ``consume()`` when
        the payload cannot be deserialised.  The caller **must** still
        XACK the original entry after calling this.
        """
        dead_data: dict[str, str] = {
            "original_stream": self.STREAM_KEY,
            "original_entry_id": entry_id,
            "consumer_group": self.GROUP_NAME,
            "data": data.get("payload", ""),
            "reason": reason,
            "dead_lettered_at": str(time.time()),
        }
        await r.xadd(self.DEAD_LETTER, dead_data)  # type: ignore[arg-type]
        logger.warning(
            "raw entry sent to dead letter",
            entry_id=entry_id,
            stream=self.DEAD_LETTER,
            reason=reason,
        )

    async def dead_letter(
        self,
        entry_id: str,
        payload: ExecutionPayload,
        reason: str = "",
    ) -> None:
        """Move a poison-pill message to the dead-letter stream."""
        r = await self._get_redis()
        dead_data: dict[str, Any] = {
            "original_stream": self.STREAM_KEY,
            "original_entry_id": entry_id,
            "consumer_group": self.GROUP_NAME,
            "payload": payload.model_dump_json(),
            "reason": reason,
            "dead_lettered_at": str(time.time()),
        }
        await r.xadd(self.DEAD_LETTER, dead_data)  # type: ignore[arg-type]
        logger.warning(
            "message sent to dead letter",
            entry_id=entry_id,
            stream=self.DEAD_LETTER,
            reason=reason,
        )

    # ------------------------------------------------------------------
    # Execution results (HASH)
    # ------------------------------------------------------------------

    async def set_result(
        self,
        execution_id: str,
        status: str,
        tx_hash: str = "",
        error: str = "",
    ) -> None:
        """Store the result of an execution."""
        r = await self._get_redis()
        mapping: dict[str, str] = {"status": status}
        if tx_hash:
            mapping["tx_hash"] = tx_hash
        if error:
            mapping["error"] = error
        await r.hset(f"{self.RESULT_PREFIX}{execution_id}", mapping=mapping)  # type: ignore[misc]
        await r.expire(f"{self.RESULT_PREFIX}{execution_id}", 86400)

    async def get_result(self, execution_id: str) -> dict[str, str] | None:
        """Read the result of an execution.

        Returns ``None`` if the result hash does not exist or is expired.
        """
        r = await self._get_redis()
        result = await r.hgetall(f"{self.RESULT_PREFIX}{execution_id}")  # type: ignore[misc]
        return result if result else None

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def stream_length(self) -> int:
        """Return the current number of entries in the execution stream."""
        r = await self._get_redis()
        length = await r.xlen(self.STREAM_KEY)
        return int(length) if length is not None else 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the Redis connection if we own it."""
        if self._owned_redis and self._redis is not None:
            await self._redis.close()
            self._redis = None
