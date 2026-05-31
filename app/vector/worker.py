import asyncio
import signal

from app.core.config import settings
from app.core.logging import get_logger
import app.core.metrics as metrics_
from app.schemas.mini_block import MiniBlockPayload
from app.schemas.vector import VectorPayload
from app.streams.consumer import StreamConsumer
from app.streams.buffer import StreamBuffer
from app.vector.embedder import CohereEmbedder
from app.vector.qdrant_client import QdrantManager
from app.vector.summarizer import build_summary

logger = get_logger("megaeth.vector.worker")


class VectorIndexerWorker:
    """Consumes mini-blocks, embeds their summaries, and indexes in Qdrant."""

    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self._consumer: StreamConsumer | None = None
        self._buffer: StreamBuffer | None = None
        self._embedder: CohereEmbedder | None = None
        self._qdrant: QdrantManager | None = None
        self._processed = 0

    async def _init(self) -> None:
        """Initialize stream, Qdrant, cohere-embed."""
        # Initialize stream and consumer group
        self._buffer = StreamBuffer()
        await self._buffer.initialize()
        await self._buffer.create_consumer_group(settings.consumer_group_qdrant)

        # Initialize Qdrant collection
        self._qdrant = QdrantManager()
        self._qdrant.initialize()

        # Initialize embedder
        self._embedder = CohereEmbedder()

        # Check cohere-embed health
        healthy = await self._embedder.health_check()
        if healthy:
            logger.info("cohere-embed service healthy")
        else:
            logger.warning("cohere-embed service unavailable, will enter degraded mode")

        # Create consumer
        self._consumer = StreamConsumer(
            group_name=settings.consumer_group_qdrant,
            consumer_name="vector-indexer-1",
            stream_name=settings.stream_name,
            block_ms=1000,
        )

        logger.info("vector indexer initialized")

    def _get_payload_id(self, payload: MiniBlockPayload) -> str:
        """Get a unique ID for the Qdrant point.

        Uses mini_block_number if available, otherwise block_number:index.
        """
        if payload.mini_block_number is not None:
            return (
                f"0x{payload.mini_block_number:x}"
                if isinstance(payload.mini_block_number, int)
                else str(payload.mini_block_number)
            )
        return f"{payload.block_number}:{payload.index}"

    async def run(self) -> None:
        """Main run loop."""
        logger.info("vector indexer worker starting")
        await self._init()

        logger.info("vector indexer worker started, waiting for blocks")

        assert self._consumer is not None, "Consumer should be initialized in _init()"
        assert self._embedder is not None, "Embedder should be initialized in _init()"
        assert self._qdrant is not None, "Qdrant manager should be initialized in _init()"

        async for entry_id, data in self._consumer.consume():
            if self._shutdown_event.is_set():
                break
            await self._process_entry(entry_id, data)

        # Shutdown
        if self._consumer:
            await self._consumer.close()
        if self._embedder:
            await self._embedder.close()
        if self._qdrant:
            self._qdrant.close()

        logger.info(
            "vector indexer worker stopped",
            blocks_processed=self._processed,
        )

    async def _process_entry(self, entry_id: str, data: dict) -> None:
        """Process a single stream entry: parse, summarize, embed, index, ack."""
        try:
            entry = StreamConsumer.parse_entry(data)
            payload = StreamConsumer.parse_mini_block(entry)

            # Build summary
            summary = build_summary(payload)
            payload_id = self._get_payload_id(payload)

            # Embed (with degraded mode)
            embedding = None
            try:
                embedding = await self._embedder.embed(summary)  # type: ignore[union-attr]
            except Exception as e:
                metrics_.degraded_events_total += 1
                logger.warning(
                    "embedding failed, entering degraded mode for this block",
                    block_number=payload.block_number,
                    error=str(e),
                )

            # Index in Qdrant
            if embedding is not None and self._qdrant is not None:
                vector_payload = VectorPayload(
                    payload_id=payload_id,
                    block_number=payload.block_number,
                    summary=summary,
                    embedding=embedding,
                    metadata={
                        "index": payload.index,
                        "gas_used": payload.gas_used,
                    },
                )
                self._qdrant.upsert(vector_payload)
                metrics_.vectors_upserted_total += 1

            self._processed += 1
            await self._consumer.ack(entry_id)  # type: ignore[union-attr]

            logger.debug(
                "block indexed",
                block_number=payload.block_number,
                index=payload.index,
                summary_len=len(summary),
                embedded=embedding is not None,
            )

        except Exception as e:
            logger.error(
                "failed to index block",
                entry_id=entry_id,
                error=str(e),
                exc_info=True,
            )
            await self._consumer.dead_letter(entry_id, data, reason=str(e))  # type: ignore[union-attr]
            await self._consumer.ack(entry_id)  # type: ignore[union-attr]

    async def shutdown(self) -> None:
        """Trigger graceful shutdown."""
        logger.info("shutdown requested")
        self._shutdown_event.set()


async def main() -> None:
    """Entrypoint for the vector indexer worker."""
    worker = VectorIndexerWorker()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(worker.shutdown()),
        )

    await worker.run()
