"""Endurance (soak) test.

Injects synthetic mini-blocks at peak rate for configured duration.
Verifies stable consumer lag and data completeness.

Run: SOAK_DURATION_SEC=30 uv run -m pytest tests/soak/ -v --soak -s
"""

import asyncio
import time

import pytest

from app.core.config import settings
from app.state.database import create_async_engine_pool
from app.state.repository import StateRepository
from app.streams.buffer import StreamBuffer
from app.streams.consumer import StreamConsumer
from app.schemas.state import MiniBlockRecord
from app.state.database import create_sync_engine

from .conftest import (
    SOAK_DURATION_SEC,
    SOAK_RATE_PER_SEC,
    SOAK_STREAM,
    SOAK_GROUP,
    generate_blocks,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.soak
class TestSoak:
    """Endurance test — inject blocks at peak rate, verify processing."""

    @staticmethod
    def pytest_collection_modifyitems(config, items):
        if not config.getoption("--soak"):
            skip = pytest.mark.skip(reason="need --soak option to run")
            for item in items:
                if "soak" in item.keywords:
                    item.add_marker(skip)

    async def test_soak_endurance(self, soak_stream):
        """Inject at SOAK_RATE_PER_SEC for SOAK_DURATION_SEC."""
        import redis.asyncio as aioredis

        r = aioredis.from_url(settings.redis_url, decode_responses=True)

        # Init DB
        sync_engine = create_sync_engine()
        MiniBlockRecord.metadata.create_all(sync_engine)
        sync_engine.dispose()

        # Init stream
        buf = StreamBuffer(redis_client=r)
        await buf.initialize()
        try:
            await buf.create_consumer_group(SOAK_GROUP)
        except Exception:
            pass

        # Consumer
        consumer = StreamConsumer(
            group_name=SOAK_GROUP,
            consumer_name="soak-1",
            stream_name=SOAK_STREAM,
            redis_client=r,
            block_ms=500,
        )

        # Async engine for PG
        engine, session_factory = create_async_engine_pool()

        producer_done = asyncio.Event()
        total_injected = 0
        total_processed = 0
        start_time = time.monotonic()

        async def produce():
            nonlocal total_injected
            batch_size = SOAK_RATE_PER_SEC
            end_time = start_time + SOAK_DURATION_SEC
            iteration = 0

            while time.monotonic() < end_time:
                blocks = generate_blocks(batch_size, start_block=iteration * batch_size)
                for corr_id, payload in blocks:
                    await buf.add(
                        block_number=payload.block_number,
                        raw_payload=payload.model_dump_json(),
                        correlation_id=corr_id,
                    )
                total_injected += len(blocks)
                iteration += 1

                if iteration % 5 == 0:
                    elapsed = time.monotonic() - start_time
                    rate = total_injected / elapsed if elapsed > 0 else 0
                    print(f"\n[SOAK] Injected {total_injected} ({rate:.0f}/sec)")

                await asyncio.sleep(1.0)

            producer_done.set()
            print(f"\n[SOAK] Producer done. Total: {total_injected}")

        async def consume():
            nonlocal total_processed
            last_log = time.monotonic()

            while True:
                done = producer_done.is_set()
                try:
                    async for eid, data in consumer.consume():
                        entry = StreamConsumer.parse_entry(data)
                        payload = StreamConsumer.parse_mini_block(entry)
                        record = StateRepository.record_from_payload(payload, entry.raw_payload)

                        async with session_factory() as session:
                            repo = StateRepository(session)
                            await repo.upsert(record)

                        await consumer.ack(eid)
                        total_processed += 1

                        now = time.monotonic()
                        if now - last_log > 30:
                            lag = total_injected - total_processed
                            pel = await buf.pending_count(SOAK_GROUP)
                            print(f"[SOAK] Processed {total_processed}, lag {lag}, PEL {pel}")
                            last_log = now
                except Exception:
                    if done:
                        break
                    await asyncio.sleep(0.1)

        await asyncio.gather(produce(), consume())

        elapsed = time.monotonic() - start_time
        async with session_factory() as session:
            repo = StateRepository(session)
            db_count = await repo.count()

        print(f"\n=== SOAK RESULTS ===")
        print(f"Duration: {elapsed:.1f}s")
        print(f"Injected: {total_injected}")
        print(f"Processed: {total_processed}")
        print(f"DB records: {db_count}")
        print(f"Lag: {total_injected - total_processed}")
        print(f"Throughput: {total_processed / elapsed:.0f} blocks/sec")

        assert total_processed >= total_injected * 0.99, "Consumer should process ≥99% of injected blocks"

        await engine.dispose()
        await consumer.close()
        await r.aclose()
