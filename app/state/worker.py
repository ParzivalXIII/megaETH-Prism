"""StateTrackerWorker — consumes mini-blocks from Redis Stream, upserts into PostgreSQL.

Entrypoint
----------
Run as a standalone process::

    python -m app.state.worker

Or import and integrate into your application lifecycle::

    worker = StateTrackerWorker()
    asyncio.create_task(worker.run())
"""

from __future__ import annotations

import asyncio
import signal

from app.core.config import settings
from app.core.logging import get_logger
from app.core.metrics import blocks_failed_total, blocks_processed_total
from app.state.database import create_async_engine_pool, create_sync_engine
from app.state.repository import MiniBlockRecord, StateRepository
from app.streams.buffer import StreamBuffer
from app.streams.consumer import StreamConsumer

logger = get_logger("megaeth.state.worker")


class StateTrackerWorker:
    """Consumes mini-blocks from Redis Stream and upserts them into PostgreSQL.

    The worker:
    1. Initialises the database schema (CREATE TABLE IF NOT EXISTS).
    2. Ensures the Redis consumer group exists.
    3. Claims orphaned messages from crashed workers.
    4. Enters the main consume→upsert→ack loop.
    5. Handles graceful shutdown on SIGTERM/SIGINT.
    """

    def __init__(self) -> None:
        self._shutdown_event = asyncio.Event()
        self._engine = None
        self._session_factory = None
        self._consumer: StreamConsumer | None = None
        self._buffer: StreamBuffer | None = None
        self._processed = 0

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    async def _init_db(self) -> None:
        """Initialize database: create tables if they don't exist.

        Uses the synchronous engine for table creation since
        ``SQLModel.metadata.create_all`` is a synchronous API.
        """
        sync_engine = create_sync_engine()
        MiniBlockRecord.metadata.create_all(sync_engine)
        sync_engine.dispose()
        logger.info("database tables initialized")

    async def _init_stream(self) -> None:
        """Ensure the stream and consumer group exist."""
        self._buffer = StreamBuffer()
        await self._buffer.initialize()
        await self._buffer.create_consumer_group(settings.consumer_group_postgres)
        logger.info(
            "consumer group ready",
            group=settings.consumer_group_postgres,
        )

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main run loop — consume from Redis Stream, upsert to PostgreSQL."""
        logger.info("state tracker worker starting")

        # Initialize dependencies
        await self._init_db()
        await self._init_stream()

        async_engine, self._session_factory = create_async_engine_pool()
        self._engine = async_engine

        # Create consumer
        self._consumer = StreamConsumer(
            group_name=settings.consumer_group_postgres,
            consumer_name="state-tracker-1",
            stream_name=settings.stream_name,
            block_ms=1000,
        )

        logger.info("state tracker worker started, waiting for blocks")

        # Consumer is guaranteed to be set above
        consumer = self._consumer
        assert consumer is not None

        # Claim any orphaned messages first
        try:
            claimed = await consumer.claim_pending(min_idle_ms=30000)
            if claimed:
                logger.info("claimed orphaned messages", count=len(claimed))
                for entry_id, data in claimed:
                    await self._process_entry(entry_id, data)
        except Exception as e:
            logger.warning("claim_pending failed on startup", error=str(e))

        # Main consume loop
        async for entry_id, data in consumer.consume():
            if self._shutdown_event.is_set():
                break
            await self._process_entry(entry_id, data)

        # Shutdown
        await self._shutdown()

        logger.info(
            "state tracker worker stopped",
            blocks_processed=self._processed,
        )

    # ------------------------------------------------------------------
    # Entry processing
    # ------------------------------------------------------------------

    async def _process_entry(self, entry_id: str, data: dict) -> None:
        """Process a single stream entry: parse, upsert, ack.

        On failure the entry is routed to the dead-letter stream before
        being acknowledged (to prevent infinite re-processing).
        """
        global blocks_processed_total, blocks_failed_total

        # Consumer and session factory guaranteed to be set by run()
        consumer = self._consumer
        assert consumer is not None
        session_factory = self._session_factory
        assert session_factory is not None

        try:
            entry = StreamConsumer.parse_entry(data)
            payload = StreamConsumer.parse_mini_block(entry)

            record = StateRepository.record_from_payload(payload, entry.raw_payload)

            async with session_factory() as session:
                repo = StateRepository(session)
                await repo.upsert(record)

            blocks_processed_total += 1
            self._processed += 1

            await consumer.ack(entry_id)

            logger.debug(
                "block upserted",
                block_number=payload.block_number,
                index=payload.index,
                tx_count=payload.tx_count,
            )

        except Exception as e:
            blocks_failed_total += 1
            logger.error(
                "failed to process block",
                entry_id=entry_id,
                error=str(e),
                exc_info=True,
            )
            # Poison pill handling: after 3 failures, move to dead letter.
            # For simplicity in Phase 1, just ack after failure.
            await consumer.dead_letter(entry_id, data, reason=str(e))
            await consumer.ack(entry_id)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def _shutdown(self) -> None:
        """Clean up resources on shutdown."""
        if self._consumer:
            await self._consumer.close()
        if self._engine:
            await self._engine.dispose()

    async def shutdown(self) -> None:
        """Trigger graceful shutdown.

        This method is safe to call from signal handlers.
        """
        logger.info("shutdown requested")
        self._shutdown_event.set()


async def main() -> None:
    """Entrypoint for the state tracker worker.

    Registers signal handlers for SIGTERM and SIGINT, then starts the
    worker's main loop.
    """
    worker = StateTrackerWorker()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(worker.shutdown()),
        )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
