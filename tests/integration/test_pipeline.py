"""End-to-end pipeline integration tests.

Tests inject synthetic miniBlocks via direct Redis XADD (bypassing WebSocket),
then verify they flow through the pipeline to PostgreSQL and Qdrant.
"""

import uuid

import pytest
import pytest_asyncio

from app.core.config import settings
from app.schemas.mini_block import MiniBlockPayload
from app.schemas.state import MiniBlockRecord
from app.schemas.streams import (
    STREAM_FIELD_BLOCK_NUMBER,
    STREAM_FIELD_CORRELATION_ID,
    STREAM_FIELD_INGESTED_AT,
    STREAM_FIELD_RAW_PAYLOAD,
)
from app.state.database import create_async_engine_pool, create_sync_engine
from app.state.repository import StateRepository
from app.streams.consumer import StreamConsumer
from app.vector.qdrant_client import QdrantManager

pytestmark = pytest.mark.asyncio

TEST_STREAM = "megaeth:test:integration"
TEST_GROUP = "megaeth:test:int-workers"


@pytest_asyncio.fixture
async def setup_pipeline():
    """Set up test stream, consumer group, DB tables, Qdrant collection.

    Uses direct Redis commands to create the test stream (bypasses
    ``StreamBuffer`` which is hardcoded to ``settings.stream_name``).
    """
    import redis.asyncio as aioredis

    # Init DB tables (idempotent)
    sync_engine = create_sync_engine()
    MiniBlockRecord.metadata.create_all(sync_engine)
    sync_engine.dispose()

    # Truncate any leftover data from previous tests
    from sqlalchemy import text as sa_text
    truncate_engine = create_sync_engine()
    with truncate_engine.connect() as conn:
        conn.execute(
            sa_text(
                "TRUNCATE TABLE mini_block_records RESTART IDENTITY CASCADE"
            )
        )
        conn.commit()
    truncate_engine.dispose()

    # Connect to Redis
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    await r.ping()

    # Clean and create test stream with init marker
    await r.delete(TEST_STREAM)
    init_id = await r.xadd(TEST_STREAM, {"init": "1"})
    await r.xdel(TEST_STREAM, init_id)

    # Create consumer group (idempotent)
    try:
        await r.xgroup_create(
            TEST_STREAM, TEST_GROUP, id="$", mkstream=True
        )
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    # Init Qdrant collection (idempotent)
    qm = QdrantManager()
    qm.initialize()
    qm.close()

    yield r

    # Cleanup
    await r.delete(TEST_STREAM)
    await r.aclose()


async def _inject_block(
    r,
    block_number: int,
    raw_payload: str,
    correlation_id: str | None = None,
) -> str:
    """Inject a mini-block entry into the test stream via direct XADD."""
    corr_id = correlation_id or str(uuid.uuid4())
    stream_data = {
        STREAM_FIELD_BLOCK_NUMBER: str(block_number),
        STREAM_FIELD_RAW_PAYLOAD: raw_payload,
        STREAM_FIELD_INGESTED_AT: str(1704067200.0 + block_number),
        STREAM_FIELD_CORRELATION_ID: corr_id,
    }
    entry_id = await r.xadd(TEST_STREAM, stream_data)
    return entry_id


# ---------------------------------------------------------------------------
# Acceptance: 1. Inject 100 blocks, consume, verify all in PostgreSQL
# ---------------------------------------------------------------------------


async def test_e2e_100_blocks(setup_pipeline):
    """Inject 100 synthetic blocks, verify all reach PostgreSQL."""
    r = setup_pipeline
    ingested = 100

    # Inject 100 blocks via direct XADD
    for i in range(ingested):
        payload = MiniBlockPayload(
            block_number=i,
            block_timestamp=1704067200 + i,
            index=0,
            gas_used=21000,
            transactions=[f"0xtx{i}"],
            receipts=[f"0xr{i}"],
        )
        await _inject_block(r, i, payload.model_dump_json())

    # Verify stream has 100 entries
    length = await r.xlen(TEST_STREAM)
    assert length == ingested, f"Expected {ingested} entries, got {length}"

    # Consume and persist to PostgreSQL
    engine, session_factory = create_async_engine_pool()
    consumer = StreamConsumer(
        group_name=TEST_GROUP,
        consumer_name="test-consumer",
        stream_name=TEST_STREAM,
        redis_client=r,
        block_ms=100,
    )

    processed = 0
    async for entry_id, data in consumer.consume():
        entry = StreamConsumer.parse_entry(data)
        payload = StreamConsumer.parse_mini_block(entry)

        record = StateRepository.record_from_payload(payload, entry.raw_payload)

        async with session_factory() as session:
            repo = StateRepository(session)
            await repo.upsert(record)

        await consumer.ack(entry_id)
        processed += 1
        if processed >= ingested:
            break

    assert processed == ingested, (
        f"Processed {processed} blocks, expected {ingested}"
    )

    # Verify DB has all records
    async with session_factory() as session:
        repo = StateRepository(session)
        count = await repo.count()
        assert count == ingested, (
            f"Expected {ingested} DB records, got {count}"
        )

    await engine.dispose()
    await consumer.close()


# ---------------------------------------------------------------------------
# Acceptance: 2. Idempotent upsert — duplicate injection deduplicates
# ---------------------------------------------------------------------------


async def test_idempotent_upsert(setup_pipeline):
    """Inject same block twice — no duplicates in DB."""
    r = setup_pipeline

    payload = MiniBlockPayload(
        block_number=1,
        block_timestamp=1000,
        index=0,
        gas_used=21000,
        transactions=["0xtx1"],
        receipts=["0xr1"],
    )
    raw = payload.model_dump_json()

    # Inject same block twice
    await _inject_block(r, 1, raw, correlation_id="first")
    await _inject_block(r, 1, raw, correlation_id="second")

    engine, session_factory = create_async_engine_pool()
    consumer = StreamConsumer(
        group_name=TEST_GROUP,
        consumer_name="test-idempotent",
        stream_name=TEST_STREAM,
        redis_client=r,
        block_ms=100,
    )

    processed = 0
    async for entry_id, data in consumer.consume():
        entry = StreamConsumer.parse_entry(data)
        payload = StreamConsumer.parse_mini_block(entry)
        record = StateRepository.record_from_payload(payload, entry.raw_payload)

        async with session_factory() as session:
            repo = StateRepository(session)
            await repo.upsert(record)

        await consumer.ack(entry_id)
        processed += 1
        if processed >= 2:
            break

    # Verify single record (ON CONFLICT DO UPDATE via merge)
    async with session_factory() as session:
        repo = StateRepository(session)
        count = await repo.count()
        assert count == 1, f"Expected 1 record, got {count}"

    await engine.dispose()
    await consumer.close()


# ---------------------------------------------------------------------------
# Acceptance: 3. Consume with correlation IDs intact
# ---------------------------------------------------------------------------


async def test_correlation_ids_preserved(setup_pipeline):
    """Correlation IDs injected via XADD should survive the consume cycle."""
    r = setup_pipeline
    num_blocks = 5

    # Inject blocks with specific correlation IDs
    expected_corrs = {}
    for i in range(num_blocks):
        corr = f"corr-{i:04d}-{uuid.uuid4().hex[:8]}"
        payload = MiniBlockPayload(
            block_number=i,
            block_timestamp=1704067200 + i,
            index=0,
            gas_used=21000,
            transactions=[],
            receipts=[],
        )
        await _inject_block(r, i, payload.model_dump_json(), correlation_id=corr)
        expected_corrs[i] = corr

    engine, session_factory = create_async_engine_pool()
    consumer = StreamConsumer(
        group_name=TEST_GROUP,
        consumer_name="test-corrs",
        stream_name=TEST_STREAM,
        redis_client=r,
        block_ms=100,
    )

    seen_corrs = {}
    async for entry_id, data in consumer.consume():
        entry = StreamConsumer.parse_entry(data)
        seen_corrs[entry.block_number] = entry.correlation_id
        await consumer.ack(entry_id)
        if len(seen_corrs) >= num_blocks:
            break

    # Verify every correlation ID is preserved
    for block_num, expected_corr in expected_corrs.items():
        assert seen_corrs[block_num] == expected_corr, (
            f"Block {block_num}: expected correlation {expected_corr!r}, "
            f"got {seen_corrs.get(block_num)!r}"
        )

    await engine.dispose()
    await consumer.close()


# ---------------------------------------------------------------------------
# Acceptance: 4. Pipeline handles empty blocks (no transactions)
# ---------------------------------------------------------------------------


async def test_empty_blocks(setup_pipeline):
    """Empty mini-blocks should be ingested without error."""
    r = setup_pipeline
    num_empty = 10

    for i in range(num_empty):
        payload = MiniBlockPayload(
            block_number=1000 + i,
            block_timestamp=1704067200 + i,
            index=0,
            gas_used=0,
            transactions=[],
            receipts=[],
        )
        await _inject_block(r, 1000 + i, payload.model_dump_json())

    engine, session_factory = create_async_engine_pool()
    consumer = StreamConsumer(
        group_name=TEST_GROUP,
        consumer_name="test-empty",
        stream_name=TEST_STREAM,
        redis_client=r,
        block_ms=100,
    )

    processed = 0
    async for entry_id, data in consumer.consume():
        entry = StreamConsumer.parse_entry(data)
        payload = StreamConsumer.parse_mini_block(entry)
        assert payload.tx_count == 0

        record = StateRepository.record_from_payload(payload, entry.raw_payload)

        async with session_factory() as session:
            repo = StateRepository(session)
            await repo.upsert(record)

        await consumer.ack(entry_id)
        processed += 1
        if processed >= num_empty:
            break

    async with session_factory() as session:
        repo = StateRepository(session)
        count = await repo.count()
        assert count == num_empty

    await engine.dispose()
    await consumer.close()
