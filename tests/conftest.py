"""Shared pytest fixtures for integration tests.

Integration tests inject data via direct Redis XADD to the test stream,
bypassing the WebSocket. This decouples tests from the MegaETH network.
"""

import asyncio
import os
from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.schemas.mini_block import MiniBlockPayload
from app.schemas.state import MiniBlockRecord
from app.state.database import create_sync_engine

# Test-specific overrides
TEST_STREAM_NAME = os.environ.get("TEST_STREAM_NAME", "megaeth:test:miniBlocks")
TEST_CONSUMER_GROUP = "megaeth:test:workers"


@pytest.fixture(scope="session")
def event_loop():
    """Create a single event loop for the session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator[aioredis.Redis, None]:
    """Connect to real Redis from Docker compose."""
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    await r.ping()
    yield r
    await r.aclose()


@pytest_asyncio.fixture
async def test_stream(redis_client: aioredis.Redis) -> AsyncGenerator[str, None]:
    """Create a test stream and consumer group, cleanup after."""
    # Delete any existing test stream
    await redis_client.delete(TEST_STREAM_NAME)

    # Create fresh stream
    init_id = await redis_client.xadd(TEST_STREAM_NAME, {"init": "1"})
    await redis_client.xdel(TEST_STREAM_NAME, init_id)

    # Create consumer group
    try:
        await redis_client.xgroup_create(
            TEST_STREAM_NAME, TEST_CONSUMER_GROUP, id="$", mkstream=True
        )
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    yield TEST_STREAM_NAME

    # Cleanup
    await redis_client.delete(TEST_STREAM_NAME)


@pytest.fixture
def mini_block_factory() -> Any:
    """Factory for creating test mini-block payloads."""

    def _create(
        block_number: int,
        index: int = 0,
        tx_count: int = 0,
        gas_used: int = 21000,
    ) -> MiniBlockPayload:
        return MiniBlockPayload(
            block_number=block_number,
            block_timestamp=1704067200 + block_number,
            index=index,
            gas_used=gas_used,
            transactions=[f"0xtx{i}" for i in range(tx_count)],
            receipts=[f"0xr{i}" for i in range(tx_count)],
        )

    return _create


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session with auto-cleanup."""
    # Initialize tables
    sync_engine = create_sync_engine()
    MiniBlockRecord.metadata.create_all(sync_engine)
    sync_engine.dispose()

    # Create async session
    engine = create_async_engine(settings.postgres_url, pool_size=2)
    factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with factory() as session:
        # Clean any existing test data
        await session.execute(
            sa_text(
                "TRUNCATE TABLE mini_block_records RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()
        yield session

    await engine.dispose()
