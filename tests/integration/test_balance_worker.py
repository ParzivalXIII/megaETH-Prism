"""Integration tests for BalanceTrackerWorker (T7).

Tests require:
- A running Redis instance (``docker compose up -d redis``)
- A running PostgreSQL instance (``docker compose up -d postgres``)

These tests inject mini-block payloads directly into a test Redis Stream
and verify that the balance worker extracts Transfer events and upserts
asset balances correctly.
"""

from __future__ import annotations

import time

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from app.core.config import settings
from app.schemas.mini_block import MiniBlockPayload
from app.schemas.streams import STREAM_FIELD_BLOCK_NUMBER, STREAM_FIELD_RAW_PAYLOAD
from app.state.balance_repository import AssetBalanceRepository
from app.state.balance_worker import BalanceTrackerWorker
from app.state.database import create_async_engine_pool, create_sync_engine
from app.state.models import AssetBalance

pytestmark = pytest.mark.asyncio

# Test stream isolation
TEST_MINI_BLOCK_STREAM = "megaeth:test:balance:miniBlocks"
TEST_BALANCE_GROUP = "megaeth:test:balance:workers"
TEST_BALANCE_DEAD = "megaeth:test:balance:dead"

# Test addresses
TOKEN_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_ALICE = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_BOB = "0xcccccccccccccccccccccccccccccccccccccccc"

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _make_transfer_log(
    token_address: str = TOKEN_A,
    from_addr: str = ADDR_ALICE,
    to_addr: str = ADDR_BOB,
    amount_hex: str = "0x64",  # 100
) -> dict:
    from_padded = "0x" + "00" * 12 + from_addr[2:]
    to_padded = "0x" + "00" * 12 + to_addr[2:]
    return {
        "topics": [TRANSFER_TOPIC, from_padded, to_padded],
        "data": amount_hex,
        "address": token_address,
    }


def _make_mini_block(
    block_number: int = 1,
    logs: list[dict] | None = None,
) -> MiniBlockPayload:
    return MiniBlockPayload(
        block_number=block_number,
        block_timestamp=1704067200 + block_number,
        index=0,
        gas_used=21000,
        transactions=["0xtx1"] if logs else [],
        receipts=[{"status": "0x1", "logs": logs or []}],
    )


async def _inject_payload(
    redis_client: aioredis.Redis,
    payload: MiniBlockPayload,
    stream: str,
) -> str:
    """Inject a MiniBlockPayload into the test stream."""
    raw_json = payload.model_dump_json()
    entry_id = await redis_client.xadd(
        stream,
        {
            STREAM_FIELD_BLOCK_NUMBER: str(payload.block_number),
            STREAM_FIELD_RAW_PAYLOAD: raw_json,
            "ingested_at": str(time.time()),
            "correlation_id": "test",
        },
    )
    return entry_id


@pytest_asyncio.fixture
async def test_setup():
    """Set up test stream, database, and cleanup."""
    # Connect to Redis
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    await r.ping()

    # Create test stream
    await r.delete(TEST_MINI_BLOCK_STREAM)
    init_id = await r.xadd(TEST_MINI_BLOCK_STREAM, {"init": "1"})
    await r.xdel(TEST_MINI_BLOCK_STREAM, init_id)

    # Create consumer group
    try:
        await r.xgroup_create(
            TEST_MINI_BLOCK_STREAM, TEST_BALANCE_GROUP, id="$", mkstream=True
        )
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    # Initialize DB tables
    sync_engine = create_sync_engine()
    AssetBalance.metadata.create_all(sync_engine)
    sync_engine.dispose()

    # Clean any leftover data
    engine, session_factory = create_async_engine_pool()
    from sqlalchemy import text as truncate_text
    try:
        async with session_factory() as session:
            await session.execute(
                truncate_text(
                    "TRUNCATE TABLE asset_balances RESTART IDENTITY CASCADE"
                )
            )
            await session.commit()
    except Exception:
        pass
    await engine.dispose()

    yield r

    # Cleanup Redis
    await r.delete(TEST_MINI_BLOCK_STREAM)
    await r.aclose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_worker_creates_consumer_group(
    test_setup: aioredis.Redis,
) -> None:
    """Start worker → consumer group exists on test stream."""
    worker = BalanceTrackerWorker(
        consumer_group=TEST_BALANCE_GROUP,
        dead_letter=TEST_BALANCE_DEAD,
        block_ms=500,
    )

    # Override stream name for the worker
    original_stream = settings.stream_name
    settings.stream_name = TEST_MINI_BLOCK_STREAM  # type: ignore[misc]

    try:
        await worker.start()

        # Verify consumer group exists
        groups = await test_setup.xinfo_groups(TEST_MINI_BLOCK_STREAM)
        group_names = [g["name"] for g in groups]
        assert TEST_BALANCE_GROUP in group_names

        await worker.stop()
    finally:
        settings.stream_name = original_stream  # type: ignore[misc]


async def test_worker_extracts_transfer_and_upserts(
    test_setup: aioredis.Redis,
) -> None:
    """Inject mini-block with 1 Transfer → balance row created for both addresses."""
    worker = BalanceTrackerWorker(
        consumer_group=TEST_BALANCE_GROUP,
        dead_letter=TEST_BALANCE_DEAD,
        block_ms=500,
    )

    original_stream = settings.stream_name
    settings.stream_name = TEST_MINI_BLOCK_STREAM  # type: ignore[misc]

    try:
        # Seed Alice with starting balance before the transfer
        engine, session_factory = create_async_engine_pool()
        async with session_factory() as session:
            repo = AssetBalanceRepository(session)
            await repo.upsert(
                AssetBalance(
                    user_address=ADDR_ALICE,
                    token_address=TOKEN_A,
                    balance_raw=100,
                    block_number=9,
                )
            )
        await engine.dispose()

        await worker.start()

        # Inject a mini-block with a Transfer event (Alice sends 100 Bob)
        payload = _make_mini_block(
            block_number=10,
            logs=[_make_transfer_log()],
        )
        await _inject_payload(test_setup, payload, TEST_MINI_BLOCK_STREAM)

        # Wait for worker to process
        import asyncio
        await asyncio.sleep(2)

        # Verify balances
        engine, session_factory = create_async_engine_pool()
        async with session_factory() as session:
            repo = AssetBalanceRepository(session)

            alice_balance = await repo.get_balance(ADDR_ALICE, TOKEN_A)
            bob_balance = await repo.get_balance(ADDR_BOB, TOKEN_A)

            # Alice: 100 - 100 = 0
            assert alice_balance is not None
            assert alice_balance.balance_raw == 0, (
                f"Alice should have 0 after sending 100 (had 100), got {alice_balance.balance_raw}"
            )

            # Bob received 100 → balance +100
            assert bob_balance is not None
            assert bob_balance.balance_raw == 100, (
                f"Bob should have 100 after receiving 100, got {bob_balance.balance_raw}"
            )

        await worker.stop()
    finally:
        settings.stream_name = original_stream  # type: ignore[misc]


async def test_worker_skips_empty_block(
    test_setup: aioredis.Redis,
) -> None:
    """Inject mini-block with no transactions → no balance rows, XACKed."""
    worker = BalanceTrackerWorker(
        consumer_group=TEST_BALANCE_GROUP,
        dead_letter=TEST_BALANCE_DEAD,
        block_ms=500,
    )

    original_stream = settings.stream_name
    settings.stream_name = TEST_MINI_BLOCK_STREAM  # type: ignore[misc]

    try:
        await worker.start()

        # Inject empty block
        payload = _make_mini_block(block_number=20, logs=[])
        await _inject_payload(test_setup, payload, TEST_MINI_BLOCK_STREAM)

        # Wait for processing
        import asyncio
        await asyncio.sleep(2)

        # Verify no balances created
        engine, session_factory = create_async_engine_pool()
        async with session_factory() as session:
            repo = AssetBalanceRepository(session)
            portfolio = await repo.get_portfolio(ADDR_ALICE)
            assert len(portfolio) == 0

        await worker.stop()
    finally:
        settings.stream_name = original_stream  # type: ignore[misc]


async def test_worker_shutdown_clean(
    test_setup: aioredis.Redis,
) -> None:
    """Start worker, inject 1 block, stop() → clean exit within 10s."""
    worker = BalanceTrackerWorker(
        consumer_group=TEST_BALANCE_GROUP,
        dead_letter=TEST_BALANCE_DEAD,
        block_ms=500,
    )

    original_stream = settings.stream_name
    settings.stream_name = TEST_MINI_BLOCK_STREAM  # type: ignore[misc]

    try:
        await worker.start()

        # Inject a block
        payload = _make_mini_block(
            block_number=30,
            logs=[_make_transfer_log()],
        )
        await _inject_payload(test_setup, payload, TEST_MINI_BLOCK_STREAM)

        import asyncio
        await asyncio.sleep(1)

        # Stop should complete cleanly within timeout
        import time
        start = time.monotonic()
        await worker.stop()
        duration = time.monotonic() - start

        assert duration < 10.0, f"Shutdown took {duration:.2f}s, expected <10s"
        assert worker.shutdown_event.is_set()
    finally:
        settings.stream_name = original_stream  # type: ignore[misc]
