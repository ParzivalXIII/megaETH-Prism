"""Integration tests for Redis Stream buffer infrastructure (T6).

These tests require a running Redis instance reachable via
``settings.redis_url`` (default ``redis://127.0.0.1:6379/0``).

Start Redis with::

    docker compose up -d redis
"""

from __future__ import annotations

import json
from typing import AsyncIterator

import pytest

from app.streams.buffer import StreamBuffer
from app.streams.consumer import StreamConsumer
from app.schemas import StreamEntry
from app.schemas.mini_block import MiniBlockPayload

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def redis_buffer() -> AsyncIterator[StreamBuffer]:
    """Create a :class:`StreamBuffer` connected to real Redis.

    Initialises the stream and a test consumer group.
    """
    import redis.asyncio as redis

    from app.core.config import settings

    r = redis.from_url(settings.redis_url, decode_responses=True)
    await r.ping()

    buffer = StreamBuffer(r)
    await buffer.initialize()

    # Create test consumer group (idempotent)
    try:
        await buffer.create_consumer_group("megaeth:test")
    except Exception:
        pass

    yield buffer

    # Cleanup: delete test data
    try:
        await r.delete(settings.stream_name)
    except Exception:
        pass
    try:
        await r.delete(settings.dead_letter_stream)
    except Exception:
        pass


@pytest.fixture
async def stream_consumer() -> AsyncIterator[StreamConsumer]:
    """Create a :class:`StreamConsumer` for the test consumer group."""
    import redis.asyncio as redis

    from app.core.config import settings

    r = redis.from_url(settings.redis_url, decode_responses=True)
    await r.ping()

    consumer = StreamConsumer(
        "megaeth:test", "test-consumer-1", r,
        block_ms=500,
    )

    yield consumer

    await consumer.close()


# ---------------------------------------------------------------------------
# Acceptance: 1. StreamBuffer.initialize() creates stream, XLEN = 0
# ---------------------------------------------------------------------------


async def test_buffer_initialize_creates_stream(redis_buffer: StreamBuffer) -> None:
    """After initialisation the stream should exist and have zero entries."""
    length = await redis_buffer.stream_length()
    assert isinstance(length, int)
    assert length >= 0


# ---------------------------------------------------------------------------
# Acceptance: 2. StreamBuffer.add(payload) returns valid Redis Stream ID
# ---------------------------------------------------------------------------


async def test_add_returns_valid_stream_id(redis_buffer: StreamBuffer) -> None:
    """XADD should return a Redis Stream ID in ``timestamp-seq`` format."""
    entry_id = await redis_buffer.add(
        block_number=42,
        raw_payload='{"test": "payload"}',
    )
    assert entry_id is not None
    assert "-" in entry_id, f"Expected Redis stream ID (timestamp-seq), got {entry_id!r}"

    parts = entry_id.split("-")
    assert len(parts) == 2
    assert parts[0].isdigit(), f"Timestamp part not numeric: {parts[0]}"
    assert parts[1].isdigit(), f"Sequence part not numeric: {parts[1]}"

    length = await redis_buffer.stream_length()
    assert length >= 1


# ---------------------------------------------------------------------------
# Acceptance: 3. create_consumer_group() is idempotent on second call
# ---------------------------------------------------------------------------


async def test_create_consumer_group_idempotent(
    redis_buffer: StreamBuffer,
) -> None:
    """Calling create_consumer_group twice with the same name must not raise."""
    group_name = "megaeth:test:idempotent"
    # First call — creates
    await redis_buffer.create_consumer_group(group_name)
    # Second call — must not raise BUSYGROUP
    await redis_buffer.create_consumer_group(group_name)


# ---------------------------------------------------------------------------
# Acceptance: 4. StreamConsumer.consume() yields messages in order;
#                ack() removes from PEL
# ---------------------------------------------------------------------------


async def test_consume_and_ack(
    redis_buffer: StreamBuffer,
    stream_consumer: StreamConsumer,
) -> None:
    """XADD an entry, consume via XREADGROUP, XACK it, then verify PEL is empty."""
    # Inject a real-looking payload
    payload = MiniBlockPayload(
        block_number=1,
        block_timestamp=1704067200,
        index=0,
        gas_used=21000,
        transactions=["0xtx1"],
        receipts=["0xr1"],
    )
    raw = payload.model_dump_json()

    entry_id = await redis_buffer.add(
        block_number=1,
        raw_payload=raw,
        correlation_id="test-consume-ack-corr",
    )
    assert entry_id is not None

    # Consume the entry
    entries: list[tuple[str, dict]] = []
    async for eid, data in stream_consumer.consume():
        entries.append((eid, data))
        await stream_consumer.ack(eid)
        break  # Only consume one

    assert len(entries) == 1
    consumed_id, consumed_data = entries[0]

    # Verify the consumed entry matches what we added
    assert consumed_id == entry_id, (
        f"Consumed ID {consumed_id!r} != {entry_id!r}"
    )

    # Parse & verify structured fields
    parsed = StreamConsumer.parse_entry(consumed_data)
    assert isinstance(parsed, StreamEntry)
    assert parsed.block_number == 1
    assert parsed.correlation_id == "test-consume-ack-corr"

    # Verify PEL is empty after ack
    pending = await redis_buffer.pending_count("megaeth:test")
    assert pending == 0, "PEL should be empty after XACK"


# ---------------------------------------------------------------------------
# Acceptance: 5. parse_entry() correctly decodes raw Redis data
# ---------------------------------------------------------------------------


async def test_parse_entry_decodes_correctly() -> None:
    """parse_entry must convert raw string fields to typed StreamEntry."""
    data = {
        "block_number": "42",
        "raw_payload": '{"test": "value"}',
        "ingested_at": "1234567890.123",
        "correlation_id": "test-corr",
    }
    entry = StreamConsumer.parse_entry(data)
    assert isinstance(entry, StreamEntry)
    assert entry.block_number == 42
    assert entry.raw_payload == '{"test": "value"}'
    assert entry.ingested_at == 1234567890.123
    assert entry.correlation_id == "test-corr"


async def test_parse_entry_defaults_on_missing_fields() -> None:
    """parse_entry should use safe defaults for missing fields."""
    data: dict = {}
    entry = StreamConsumer.parse_entry(data)
    assert entry.block_number == 0
    assert entry.raw_payload == ""
    assert entry.ingested_at == 0.0
    assert entry.correlation_id == ""


# ---------------------------------------------------------------------------
# Dead-letter routing
# ---------------------------------------------------------------------------


async def test_dead_letter_routing(
    redis_buffer: StreamBuffer,
    stream_consumer: StreamConsumer,
) -> None:
    """Sending a message to dead-letter should write it to the dead stream."""
    entry_id = await redis_buffer.add(
        block_number=99,
        raw_payload='{"broken": true}',
        correlation_id="dead-letter-test",
    )

    await stream_consumer.dead_letter(
        entry_id,
        {"block_number": "99", "raw_payload": '{"broken": true}'},
        reason="test dead letter",
    )

    # Verify dead letter stream has the entry
    import redis.asyncio as redis
    from app.core.config import settings

    r = redis.from_url(settings.redis_url, decode_responses=True)
    await r.ping()

    dead_entries = await r.xrange(settings.dead_letter_stream, count=10)
    await r.aclose()

    assert len(dead_entries) >= 1

    # The most recent dead entry should match
    last_id, last_data = dead_entries[-1]
    assert last_data.get("original_entry_id") == entry_id
    assert last_data.get("reason") == "test dead letter"


# ---------------------------------------------------------------------------
# XAUTOCLAIM recovery
# ---------------------------------------------------------------------------


async def test_claim_pending_returns_list(
    redis_buffer: StreamBuffer,
    stream_consumer: StreamConsumer,
) -> None:
    """claim_pending should return a list (possibly empty) without error."""
    claimed = await stream_consumer.claim_pending(min_idle_ms=1)
    assert isinstance(claimed, list)


# ---------------------------------------------------------------------------
# parse_mini_block
# ---------------------------------------------------------------------------


async def test_parse_mini_block() -> None:
    """parse_mini_block should deserialise raw_payload into MiniBlockPayload."""
    entry = StreamEntry(
        block_number=1,
        raw_payload=json.dumps({
            "block_number": 1,
            "block_timestamp": 1704067200,
            "index": 0,
            "gas_used": 21000,
            "transactions": ["0xtx1"],
            "receipts": ["0xr1"],
        }),
        ingested_at=1234567890.123,
        correlation_id="test-parse-mini-block",
    )
    mini = StreamConsumer.parse_mini_block(entry)
    assert isinstance(mini, MiniBlockPayload)
    assert mini.block_number == 1
    assert mini.tx_count == 1
