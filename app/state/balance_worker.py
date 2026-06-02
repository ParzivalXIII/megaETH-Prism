"""BalanceTrackerWorker — consumes mini-blocks from Phase 1 stream, extracts
Transfer events, and upserts asset balances.

This is the integration point between the existing Phase 1 ingestion pipeline
and the Phase 2 asset tracking system.  It consumes from the **same** Redis
Stream that ``StateTrackerWorker`` and ``VectorIndexerWorker`` use — just a
new consumer group.

Design:
- Follows the Phase 1 worker lifecycle pattern (init_db, init_stream, consume loop).
- Uses ``StreamConsumer`` for consistent XREADGROUP/XACK/claim_pending/dead_letter.
- Process loop: parse entry → parse mini_block → extract_transfers →
  apply_deltas_and_upsert → ack.
- Graceful shutdown via ``asyncio.Event`` matching Phase 1 pattern.
"""

from __future__ import annotations

import asyncio
from typing import Any

import redis.asyncio as redis

from app.core.config import settings
from app.core.logging import get_logger
from app.core.metrics import (
    transfer_events_failed_total,
    transfer_events_parsed_total,
    transfer_events_skipped_total,
)
from app.state.balance_repository import AssetBalanceRepository
from app.state.database import create_async_engine_pool, create_sync_engine
from app.state.models import AssetBalance, TokenTransfer
from app.state.transfer_parser import apply_deltas, extract_transfers
from app.streams.consumer import StreamConsumer

logger = get_logger("megaeth.state.balance_worker")


class BalanceTrackerWorker:
    """Consumes mini-blocks from Redis Stream and upserts asset balances.

    Lifecycle:
    1. ``__init__()`` — store configuration.
    2. ``_init_db()`` — create tables if not exist.
    3. ``_init_stream()`` — ensure consumer group exists.
    4. ``start()`` — begin the consumption loop.
    5. ``stop()`` — trigger graceful shutdown.
    """

    def __init__(
        self,
        consumer_group: str | None = None,
        dead_letter: str | None = None,
        block_ms: int = 1000,
    ) -> None:
        self._shutdown_event = asyncio.Event()
        self._consumer_group = consumer_group or settings.balance_consumer_group
        self._dead_letter = dead_letter or "megaeth:raw:miniBlocks:balance_dead"
        self._block_ms = block_ms
        self._consumer: StreamConsumer | None = None
        self._engine = None
        self._session_factory = None
        self._task: asyncio.Task[Any] | None = None
        self._redis_conn: redis.Redis | None = None

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the balance tracker worker.

        Initialises dependencies and spawns the processing loop.
        """
        await self._init_db()
        await self._init_stream()

        async_engine, self._session_factory = create_async_engine_pool()  # type: ignore[no-untyped-call]
        self._engine = async_engine

        self._consumer = StreamConsumer(
            group_name=self._consumer_group,
            consumer_name="balance-tracker-1",
            stream_name=settings.stream_name,
            dead_letter_stream=self._dead_letter,
            block_ms=self._block_ms,
        )
        self._consumer._shutdown_event = self._shutdown_event

        self._task = asyncio.create_task(self._process_loop())
        logger.info("balance tracker worker started")

    async def stop(self) -> None:
        """Trigger graceful shutdown."""
        logger.info("balance tracker worker stopping")
        self._shutdown_event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("balance tracker worker stop timed out")
                self._task.cancel()
        if self._consumer:
            await self._consumer.close()
        if self._engine:
            await self._engine.dispose()
        logger.info("balance tracker worker stopped")

    @property
    def shutdown_event(self) -> asyncio.Event:
        """Public shutdown event for supervisor integration."""
        return self._shutdown_event

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    async def _init_db(self) -> None:
        """Create AssetBalance + TokenTransfer tables if they don't exist."""
        sync_engine = create_sync_engine()  # type: ignore[no-untyped-call]
        AssetBalance.metadata.create_all(sync_engine)
        TokenTransfer.metadata.create_all(sync_engine)
        sync_engine.dispose()
        logger.info("balance tracker database tables initialized")

    async def _init_stream(self) -> None:
        """Ensure the consumer group exists on the main miniBlocks stream."""
        r = await self._get_redis()
        try:
            await r.xgroup_create(
                settings.stream_name,
                self._consumer_group,
                id="$",
                mkstream=True,
            )
            logger.info(
                "balance consumer group created",
                group=self._consumer_group,
                stream=settings.stream_name,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
            logger.debug(
                "balance consumer group already exists",
                group=self._consumer_group,
            )

    async def _get_redis(self) -> redis.Redis:
        """Lazy Redis connection for internal use."""
        if self._redis_conn is None:
            self._redis_conn = redis.from_url(  # type: ignore[no-untyped-call]
                settings.redis_url, decode_responses=True
            )
            await self._redis_conn.ping()
        return self._redis_conn

    # ------------------------------------------------------------------
    # Processing loop
    # ------------------------------------------------------------------

    async def _process_loop(self) -> None:
        """Main processing loop: consume → parse → extract → upsert → ack."""
        global transfer_events_parsed_total, transfer_events_failed_total, transfer_events_skipped_total

        consumer = self._consumer
        assert consumer is not None
        session_factory = self._session_factory
        assert session_factory is not None

        # Claim orphaned messages on startup
        try:
            claimed = await consumer.claim_pending(min_idle_ms=30000)
            if claimed:
                logger.info(
                    "claimed orphaned balance messages", count=len(claimed)
                )
                for entry_id, data in claimed:
                    await self._process_entry(entry_id, data, session_factory, consumer)
        except Exception as e:
            logger.warning(
                "claim_pending failed on startup for balance worker",
                error=str(e),
            )

        # Main consume loop
        async for entry_id, data in consumer.consume():
            if self._shutdown_event.is_set():
                break
            await self._process_entry(entry_id, data, session_factory, consumer)

        logger.info("balance tracker worker process loop ended")

    async def _process_entry(
        self,
        entry_id: str,
        data: dict[str, Any],
        session_factory: Any,
        consumer: StreamConsumer,
    ) -> None:
        """Process a single stream entry: parse, extract transfers, upsert, ack."""
        global transfer_events_parsed_total, transfer_events_failed_total, transfer_events_skipped_total

        try:
            # Parse entry
            entry = StreamConsumer.parse_entry(data)
            payload = StreamConsumer.parse_mini_block(entry)

            # Extract Transfer events (with skip counting for metrics)
            skip_count = 0

            def _on_skip() -> None:
                nonlocal skip_count
                skip_count += 1

            deltas = extract_transfers(payload, on_skip=_on_skip)
            transfer_events_parsed_total += len(deltas)
            transfer_events_skipped_total += skip_count

            if not deltas:
                # No transfers — just ack and continue
                await consumer.ack(entry_id)
                return

            # Aggregate deltas
            aggregated = apply_deltas(deltas)

            if not aggregated:
                await consumer.ack(entry_id)
                return

            # Upsert balances
            async with session_factory() as session:
                repo = AssetBalanceRepository(session)
                await repo.apply_deltas_and_upsert(
                    aggregated, payload.block_number
                )

            logger.debug(
                "balance upserted from transfers",
                block_number=payload.block_number,
                transfer_count=len(deltas),
                delta_count=len(aggregated),
            )

            await consumer.ack(entry_id)

        except Exception as e:
            transfer_events_failed_total += 1
            logger.error(
                "failed to process entry for balance tracking",
                entry_id=entry_id,
                error=str(e),
                exc_info=True,
            )
            await consumer.dead_letter(entry_id, data, reason=str(e))
            await consumer.ack(entry_id)
