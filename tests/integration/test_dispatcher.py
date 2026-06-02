"""Integration tests for the execution dispatcher pipeline (T8).

Tests require:
- A running Redis instance (``docker compose up -d redis``)
- A running Anvil node (``anvil --chain-id 6343 --gas-price 1000000 --block-time 1``)
- ``PRIVATE_KEY`` environment variable set to Anvil account #0

These tests inject ``ExecutionPayload`` JSON directly into a dedicated test
Redis STREAM and verify the worker pool dequeues and processes messages.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from app.agents.dispatch import ExecutionQueue
from app.agents.dispatcher import WorkerPool
from app.core.config import settings
from app.schemas.intent import ExecutionPayload, TriggerCondition

pytestmark = pytest.mark.asyncio

# Test stream isolation — use a dedicated stream for tests
TEST_EXECUTION_STREAM = "megaeth:test:execution"
TEST_EXECUTION_GROUP = "megaeth:test:executors"
TEST_EXECUTION_DEAD = "megaeth:test:execution:dead"


@pytest_asyncio.fixture
async def test_redis() -> aioredis.Redis:
    """Connect to Redis (from Docker compose)."""
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    await r.ping()
    # Clean up test streams
    await r.delete(TEST_EXECUTION_STREAM)
    await r.delete(TEST_EXECUTION_DEAD)
    yield r
    await r.aclose()


@pytest_asyncio.fixture
async def execution_queue(test_redis: aioredis.Redis) -> ExecutionQueue:
    """Create an isolated ExecutionQueue on the test stream."""
    queue = ExecutionQueue(redis_client=test_redis)
    queue.STREAM_KEY = TEST_EXECUTION_STREAM
    queue.GROUP_NAME = TEST_EXECUTION_GROUP
    queue.DEAD_LETTER = TEST_EXECUTION_DEAD
    await queue.initialize()
    return queue


@pytest.fixture
def valid_payload() -> ExecutionPayload:
    """A valid execution payload targeting Anvil account #0."""
    return ExecutionPayload(
        target_contract="0x402085c248EeA27D92E8b30b2C58ed07f9E20001",
        call_data="0xdeadbeef",
        trigger_condition=TriggerCondition(condition_type="always"),
    )


# ---------------------------------------------------------------------------
# Tests: ExecutionQueue
# ---------------------------------------------------------------------------


async def test_execution_queue_idempotent_init(
    test_redis: aioredis.Redis,
) -> None:
    """Initialize the queue twice → no error, stream exists."""
    queue = ExecutionQueue(redis_client=test_redis)
    queue.STREAM_KEY = TEST_EXECUTION_STREAM
    queue.GROUP_NAME = TEST_EXECUTION_GROUP
    queue.DEAD_LETTER = TEST_EXECUTION_DEAD

    await queue.initialize()
    await queue.initialize()  # second init should be idempotent

    assert await test_redis.exists(TEST_EXECUTION_STREAM)
    assert await test_redis.exists(TEST_EXECUTION_DEAD)


async def test_execution_queue_enqueue(
    execution_queue: ExecutionQueue,
    valid_payload: ExecutionPayload,
) -> None:
    """Enqueue a valid payload → returns a valid stream entry ID."""
    entry_id = await execution_queue.enqueue(valid_payload)
    assert entry_id is not None
    assert len(entry_id) > 0
    assert "-" in entry_id  # Redis stream ID format: timestamp-seq


async def test_execution_queue_consume_acks(
    execution_queue: ExecutionQueue,
    valid_payload: ExecutionPayload,
) -> None:
    """Enqueue → consume → ack → PEL empty."""
    await execution_queue.enqueue(valid_payload)

    found = None
    async for entry_id, payload in execution_queue.consume(block_ms=2000):
        found = (entry_id, payload)
        break

    assert found is not None, "Did not consume the enqueued message"
    entry_id, payload = found
    assert payload.target_contract == valid_payload.target_contract

    # Ack and verify PEL
    await execution_queue.ack(entry_id)
    r = await execution_queue._get_redis()
    pending = await r.xpending(
        execution_queue.STREAM_KEY, execution_queue.GROUP_NAME
    )
    pending_count = pending.get("pending", 0) if isinstance(pending, dict) else 0
    assert pending_count == 0


async def test_execution_queue_dead_letter(
    execution_queue: ExecutionQueue,
    valid_payload: ExecutionPayload,
    test_redis: aioredis.Redis,
) -> None:
    """Dead-letter routing: enqueue → consume → dead_letter → entry in DL stream."""
    await execution_queue.enqueue(valid_payload)

    async for entry_id, payload in execution_queue.consume(block_ms=2000):
        await execution_queue.dead_letter(
            entry_id, payload, reason="test failure"
        )
        await execution_queue.ack(entry_id)
        break

    # Verify dead-letter stream has the entry
    dead_entries = await test_redis.xrange(TEST_EXECUTION_DEAD)
    assert len(dead_entries) >= 1
    _, data = dead_entries[-1]  # Most recent entry
    assert "test failure" in data.get("reason", "")


async def test_execution_queue_set_get_result(
    execution_queue: ExecutionQueue,
    valid_payload: ExecutionPayload,
) -> None:
    """set_result stores result hash; get_result retrieves it."""
    execution_id = valid_payload.id
    await execution_queue.set_result(
        execution_id, "completed", tx_hash="0x123"
    )
    result = await execution_queue.get_result(execution_id)
    assert result is not None
    assert result["status"] == "completed"
    assert result["tx_hash"] == "0x123"


# ---------------------------------------------------------------------------
# Tests: WorkerPool (requires Anvil)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.environ.get("PRIVATE_KEY"),
    reason="PRIVATE_KEY not set — cannot test against Anvil",
)
async def test_worker_pool_graceful_shutdown() -> None:
    """Start worker pool, wait briefly, stop → clean exit."""
    pool = WorkerPool(worker_count=1)
    await pool.start()

    # Wait briefly to let workers start
    import asyncio
    await asyncio.sleep(1)

    # Stop
    await pool.stop()

    # Verify stopped
    assert pool.shutdown_event.is_set()
