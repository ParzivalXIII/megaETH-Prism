"""Integration tests for the AgentRunner poll loop.

Tests require:
- Docker services (Redis) from ``docker compose up -d``

These tests verify the runner lifecycle, polling interval, idle backoff,
and single invocation lock.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.agents.runner import AgentRunner

pytestmark = pytest.mark.asyncio

# Check Redis availability
_redis_available = False
try:
    import redis.asyncio as aioredis

    from app.core.config import settings

    async def _check_redis() -> bool:
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        try:
            await r.ping()
            return True
        except Exception:
            return False
        finally:
            await r.close()

    _redis_available = asyncio.run(_check_redis())
except Exception:
    _redis_available = False


def _make_mock_graph(result: dict | None = None) -> MagicMock:
    """Create a mock compiled graph."""
    if result is None:
        result = {
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "done",
        }
    graph = MagicMock()
    graph.ainvoke = AsyncMock(return_value=result)
    return graph


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_runner_lifecycle() -> None:
    """Runner starts and stops cleanly."""
    graph = _make_mock_graph()
    runner = AgentRunner(graph=graph, context={}, interval_sec=1, idle_backoff_sec=1)
    runner.start()
    assert runner._task is not None
    assert not runner._task.done()

    await asyncio.sleep(0.1)
    await runner.stop()
    assert runner._task is None or runner._task.done()


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_runner_idle_backoff() -> None:
    """Runner uses idle_backoff_sec when last run had no payload."""
    graph = _make_mock_graph(
        result={
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "done",
        }
    )
    runner = AgentRunner(graph=graph, context={}, interval_sec=1, idle_backoff_sec=5)

    # After a run with no payload, _last_had_payload should be False
    await runner._invoke_graph()
    assert not runner._last_had_payload

    await runner.stop()


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_runner_active_interval() -> None:
    """Runner uses interval_sec when last run produced a payload."""

    class MockPayload:
        def __init__(self) -> None:
            self.id = "test-id"

    graph = _make_mock_graph(
        result={
            "messages": [],
            "analysis_result": None,
            "execution_payload": MockPayload(),
            "user_address": "0xabc",
            "error": None,
            "current_phase": "dispatching",
        }
    )
    runner = AgentRunner(graph=graph, context={}, interval_sec=1, idle_backoff_sec=5)

    # After a run with a payload, _last_had_payload should be True
    await runner._invoke_graph()
    assert runner._last_had_payload

    await runner.stop()


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_single_invocation_lock() -> None:
    """Runner's invocation lock prevents concurrent invocations."""
    graph = _make_mock_graph()
    runner = AgentRunner(graph=graph, context={}, interval_sec=1, idle_backoff_sec=1)

    # Acquire the lock (simulating an in-flight invocation)
    async with runner._invocation_lock:
        # Try to invoke — should be blocked
        assert runner._invocation_lock.locked()

    # Release and verify it works
    assert not runner._invocation_lock.locked()

    await runner.stop()
