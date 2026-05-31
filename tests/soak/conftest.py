"""Soak test fixtures."""

import os
import time
import uuid
from typing import Any

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from app.core.config import settings
from app.schemas.mini_block import MiniBlockPayload

# Soak test config
SOAK_DURATION_SEC = int(os.environ.get("SOAK_DURATION_SEC", "300"))
SOAK_RATE_PER_SEC = int(os.environ.get("SOAK_RATE_PER_SEC", "100"))
SOAK_STREAM = "megaeth:soak:miniBlocks"
SOAK_GROUP = "megaeth:soak:workers"


def pytest_addoption(parser):
    parser.addoption("--soak", action="store_true", default=False, help="Run soak tests")


@pytest.fixture
def soak_config(request) -> dict[str, Any]:
    """Return soak test configuration."""
    return {
        "duration_sec": SOAK_DURATION_SEC,
        "rate_per_sec": SOAK_RATE_PER_SEC,
        "stream": SOAK_STREAM,
        "group": SOAK_GROUP,
    }


@pytest_asyncio.fixture
async def soak_stream() -> Any:
    """Create a dedicated soak test stream with consumer group."""
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    await r.delete(SOAK_STREAM)

    init_id = await r.xadd(SOAK_STREAM, {"init": "1"})
    await r.xdel(SOAK_STREAM, init_id)

    try:
        await r.xgroup_create(SOAK_STREAM, SOAK_GROUP, id="$", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    yield SOAK_STREAM
    await r.delete(SOAK_STREAM)
    await r.aclose()


def generate_blocks(count: int, start_block: int = 0) -> list[tuple[str, MiniBlockPayload]]:
    """Generate a batch of mini-block payloads."""
    blocks = []
    for i in range(count):
        payload = MiniBlockPayload(
            block_number=start_block + i,
            block_timestamp=int(time.time()),
            index=0,
            gas_used=21000 + (i % 10) * 1000,
            transactions=[f"0xtx{j}" for j in range(i % 5)],
            receipts=[f"0xr{j}" for j in range(i % 5)],
        )
        blocks.append((str(uuid.uuid4()), payload))
    return blocks
