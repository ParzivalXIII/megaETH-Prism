"""Redis Stream buffer infrastructure — lifecycle management, MAXLEN, XADD.

StreamBuffer
============
Owns the Redis Stream lifecycle: initialization, consumer group creation,
MAXLEN enforcement via XTRIM, and message injection (XADD).  All stream
operations use the canonical field-name constants defined in
:mod:`app.schemas.streams`.

Usage::

    buffer = StreamBuffer()
    await buffer.initialize()
    await buffer.create_consumer_group("megaeth:workers:postgres")
    entry_id = await buffer.add(block_number=42, raw_payload=payload_json)
    await buffer.close()
"""

from __future__ import annotations

import time
import uuid

import redis.asyncio as redis

from app.core.config import settings
from app.core.logging import get_logger
from app.schemas import (
    STREAM_FIELD_BLOCK_NUMBER,
    STREAM_FIELD_CORRELATION_ID,
    STREAM_FIELD_INGESTED_AT,
    STREAM_FIELD_RAW_PAYLOAD,
)

logger = get_logger("megaeth.streams.buffer")


class StreamBuffer:
    """Redis Stream lifecycle manager.

    Responsibilities
    ----------------
    - **Initialisation** — Ensure the stream (and dead-letter stream) exists
      with MAXLEN enforced.
    - **Consumer groups** — Create consumer groups idempotently.
    - **Injection** — XADD entries with canonical field names.

    Connection management
    ---------------------
    If *redis_client* is provided the caller is responsible for closing it.
    Otherwise a lazy connection is created from ``settings.redis_url`` and
    owned by this instance.
    """

    def __init__(self, redis_client: redis.Redis | None = None) -> None:
        self._redis: redis.Redis | None = redis_client
        self._owned_redis = redis_client is None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def _get_redis(self) -> redis.Redis:
        """Lazy Redis connection."""
        if self._redis is None:
            self._redis = redis.from_url(
                settings.redis_url, decode_responses=True
            )
            await self._redis.ping()
        return self._redis

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Ensure the stream exists with MAXLEN enforced.

        If the stream already exists, runs XTRIM to enforce the configured
        MAXLEN limit.  If the stream does **not** exist, creates it with a
        single init marker and removes that marker so the stream length is
        zero.

        The same logic is applied to the dead-letter stream.
        """
        r = await self._get_redis()
        stream = settings.stream_name

        # Check if stream exists
        exists = await r.exists(stream)
        if not exists:
            # Create stream with init marker, then remove it
            init_id = await r.xadd(
                stream, {"init": "1"},
                maxlen=settings.stream_maxlen, approximate=True,
            )
            await r.xdel(stream, init_id)
            logger.info("stream created", stream=stream,
                        maxlen=settings.stream_maxlen)
        else:
            # Retroactively enforce MAXLEN on existing stream
            before = await r.xlen(stream)
            await r.xtrim(stream, maxlen=settings.stream_maxlen,
                          approximate=True)
            after = await r.xlen(stream)
            if before != after:
                logger.info("stream trimmed", stream=stream,
                            before=before, after=after)
            else:
                logger.debug("stream within limits", stream=stream,
                             length=after)

        # Also ensure dead letter stream exists
        dead_stream = settings.dead_letter_stream
        if not await r.exists(dead_stream):
            init_id = await r.xadd(dead_stream, {"init": "1"})
            await r.xdel(dead_stream, init_id)
            logger.info("dead letter stream created", stream=dead_stream)

    async def create_consumer_group(self, group_name: str) -> None:
        """Create a consumer group for the stream.

        Idempotent — handles the ``BUSYGROUP`` error gracefully so the
        caller can safely call this on every worker restart.
        """
        r = await self._get_redis()
        try:
            await r.xgroup_create(
                settings.stream_name,
                group_name,
                id="$",          # Start consuming new messages only
                mkstream=True,
            )
            logger.info("consumer group created",
                        group=group_name, stream=settings.stream_name)
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug("consumer group already exists",
                             group=group_name)
            else:
                raise

    # ------------------------------------------------------------------
    # XADD — message injection
    # ------------------------------------------------------------------

    async def add(
        self,
        block_number: int,
        raw_payload: str,
        correlation_id: str | None = None,
    ) -> str:
        """Inject a mini-block entry into the stream.

        Parameters
        ----------
        block_number:
            MegaETH block number.
        raw_payload:
            JSON-encoded ``MiniBlockPayload``.
        correlation_id:
            Opaque tracing ID.  Auto-generated if omitted.

        Returns
        -------
        str
            The Redis stream entry ID (``{timestamp}-{seq}``) that can be
            used for acknowledgements and dead-letter routing.
        """
        r = await self._get_redis()
        corr_id = correlation_id or str(uuid.uuid4())

        stream_data = {
            STREAM_FIELD_BLOCK_NUMBER: str(block_number),
            STREAM_FIELD_RAW_PAYLOAD: raw_payload,
            STREAM_FIELD_INGESTED_AT: str(time.time()),
            STREAM_FIELD_CORRELATION_ID: corr_id,
        }

        entry_id = await r.xadd(
            settings.stream_name,
            stream_data,  # type: ignore[arg-type]
            maxlen=settings.stream_maxlen,
            approximate=True,
        )

        return entry_id

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def stream_length(self) -> int:
        """Return the current number of entries in the stream."""
        r = await self._get_redis()
        return await r.xlen(settings.stream_name)

    async def pending_count(self, group_name: str) -> int:
        """Return the number of pending (unacknowledged) messages.

        Returns 0 if the consumer group does not exist yet.
        """
        r = await self._get_redis()
        try:
            info = await r.xpending(settings.stream_name, group_name)
            return info.get("pending", 0) if isinstance(info, dict) else 0
        except redis.ResponseError:
            return 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the Redis connection if we own it."""
        if self._owned_redis and self._redis is not None:
            await self._redis.close()
            self._redis = None
