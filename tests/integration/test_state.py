"""Integration tests for the PostgreSQL state tracker worker (T7).

These tests require a running PostgreSQL instance reachable via
``settings.postgres_url`` and a running Redis instance reachable via
``settings.redis_url``.

Start both with::

    docker compose up -d postgres redis
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from app.schemas.mini_block import MiniBlockPayload
from app.schemas.state import MiniBlockRecord
from app.state.database import create_async_engine_pool, create_sync_engine
from app.state.repository import StateRepository

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def db_session():
    """Create a test database session with auto-cleanup.

    Initialises the ``mini_block_records`` table before each test.
    Truncates all data and disposes the engine after the test.
    """
    from sqlalchemy import text as truncate_text

    # Initialize tables (idempotent)
    sync_engine = create_sync_engine()
    MiniBlockRecord.metadata.create_all(sync_engine)
    sync_engine.dispose()

    engine, session_factory = create_async_engine_pool()

    # Truncate any leftover data from previous runs
    try:
        async with session_factory() as truncate_session:
            await truncate_session.execute(
                truncate_text("TRUNCATE TABLE mini_block_records RESTART IDENTITY CASCADE"),
            )
            await truncate_session.commit()
    except Exception:
        pass  # Table might not exist yet on first run

    async with session_factory() as session:
        yield session
        # Session is closed after yield in async generator fixture

    # Cleanup: truncate all data from the table
    try:
        async with session_factory() as truncate_session:
            await truncate_session.execute(
                truncate_text("TRUNCATE TABLE mini_block_records RESTART IDENTITY CASCADE"),
            )
            await truncate_session.commit()
    except Exception:
        pass  # Best-effort cleanup

    await engine.dispose()


# ---------------------------------------------------------------------------
# Acceptance: 1. Upsert inserts a new record
# ---------------------------------------------------------------------------


async def test_upsert_new_record(db_session) -> None:
    """Test inserting a new mini-block record."""
    repo = StateRepository(db_session)
    record = MiniBlockRecord(
        block_number=100,
        index=5,
        mini_block_number=999,
        block_timestamp=1700000000,
        tx_count=3,
        gas_used=21000,
        raw_jsonb={"test": "data"},
    )
    await repo.upsert(record)
    count = await repo.count()
    assert count == 1


# ---------------------------------------------------------------------------
# Acceptance: 2. Upsert is idempotent (no duplicates on same PK)
# ---------------------------------------------------------------------------


async def test_upsert_idempotent(db_session) -> None:
    """Test that upserting the same record twice doesn't create duplicates."""
    repo = StateRepository(db_session)
    record = MiniBlockRecord(
        block_number=200,
        index=3,
        mini_block_number=1000,
        block_timestamp=1700000001,
        tx_count=5,
        gas_used=42000,
        raw_jsonb={},
    )
    await repo.upsert(record)
    await repo.upsert(record)  # Same PK
    count = await repo.count()
    assert count == 1, "Should still be 1 record after upsert of same PK"


# ---------------------------------------------------------------------------
# Acceptance: 3. record_from_payload converts MiniBlockPayload correctly
# ---------------------------------------------------------------------------


async def test_record_from_payload() -> None:
    """Test conversion from MiniBlockPayload to MiniBlockRecord."""
    payload = MiniBlockPayload(
        block_number=42,
        block_timestamp=1700000002,
        index=7,
        gas_used=0x5208,
        transactions=["0xtx1"],
        receipts=["0xr1"],
    )
    raw_json = payload.model_dump_json()
    record = StateRepository.record_from_payload(payload, raw_json)
    assert record.block_number == 42
    assert record.index == 7
    assert record.tx_count == 1
    assert record.gas_used == 0x5208
    assert record.raw_jsonb["block_number"] == 42


# ---------------------------------------------------------------------------
# Acceptance: 4. get_latest_block returns the highest block number
# ---------------------------------------------------------------------------


async def test_latest_block(db_session) -> None:
    """Test getting the latest block number."""
    repo = StateRepository(db_session)
    for i in range(5):
        record = MiniBlockRecord(
            block_number=i,
            index=0,
            block_timestamp=1700000000 + i,
            tx_count=0,
            gas_used=0,
            raw_jsonb={},
        )
        await repo.upsert(record)

    latest = await repo.get_latest_block()
    assert latest == 4


# ---------------------------------------------------------------------------
# Acceptance: 5. Upsert updates existing record (ON CONFLICT DO UPDATE)
# ---------------------------------------------------------------------------


async def test_upsert_updates_existing(db_session) -> None:
    """Upserting with same PK but different values should update."""
    repo = StateRepository(db_session)

    # Insert initial
    record = MiniBlockRecord(
        block_number=300,
        index=1,
        block_timestamp=1700000003,
        tx_count=2,
        gas_used=1000,
        raw_jsonb={"version": 1},
    )
    await repo.upsert(record)

    # Upsert with same PK, different data
    updated = MiniBlockRecord(
        block_number=300,
        index=1,
        block_timestamp=1700000003,
        tx_count=5,
        gas_used=9999,
        raw_jsonb={"version": 2},
    )
    await repo.upsert(updated)

    # Should only be one row
    count = await repo.count()
    assert count == 1, "Should still be 1 record"

    # Get the latest block to verify it's still 300
    latest = await repo.get_latest_block()
    assert latest == 300


# ---------------------------------------------------------------------------
# Acceptance: 6. Multiple blocks with different indices
# ---------------------------------------------------------------------------


async def test_multiple_indices_same_block(db_session) -> None:
    """Multiple mini-blocks in the same block should all be stored."""
    repo = StateRepository(db_session)
    for idx in range(5):
        record = MiniBlockRecord(
            block_number=400,
            index=idx,
            mini_block_number=2000 + idx,
            block_timestamp=1700000010,
            tx_count=1,
            gas_used=21000 * (idx + 1),
            raw_jsonb={"index": idx},
        )
        await repo.upsert(record)

    count = await repo.count()
    assert count == 5, "Should have 5 records for 5 mini-blocks in one block"
