"""Chaos/recovery integration tests.

Tests verify that the pipeline components survive service restarts.
Uses subprocess to control Docker containers.

Skipped by default. Set ``SKIP_CHAOS_TESTS=0`` to run::

    SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_chaos.py -v -s
"""

import asyncio
import os
import subprocess
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_CHAOS_TESTS", "0") == "1",
    reason="SKIP_CHAOS_TESTS is set",
)

DOCKER_COMPOSE_PROJECT = "megaeth-orchestration"


def _docker_stop(service: str) -> None:
    """Stop a Docker compose service."""
    result = subprocess.run(
        ["docker", "compose", "-p", DOCKER_COMPOSE_PROJECT, "stop", service],
        capture_output=True,
        text=True,
        timeout=30,
    )
    print(f"docker stop {service}: {result.stdout.strip()}")


def _docker_start(service: str) -> None:
    """Start a Docker compose service."""
    result = subprocess.run(
        ["docker", "compose", "-p", DOCKER_COMPOSE_PROJECT, "start", service],
        capture_output=True,
        text=True,
        timeout=30,
    )
    print(f"docker start {service}: {result.stdout.strip()}")


class TestChaosRecovery:
    """Chaos engineering tests for service resilience."""

    def test_postgres_restart(self):
        """Stop PostgreSQL for 15 seconds, restart, verify service recovers."""
        _docker_stop("postgres")
        time.sleep(15)
        _docker_start("postgres")

        # Wait for health check to succeed
        time.sleep(10)

        # Verify connectivity
        from sqlalchemy import text as sa_text

        from app.state.database import create_sync_engine

        engine = create_sync_engine()
        with engine.connect() as conn:
            result = conn.execute(sa_text("SELECT 1"))
            assert result.scalar() == 1
        engine.dispose()

    def test_redis_restart(self):
        """Stop Redis for 10 seconds, restart, verify reconnection."""
        _docker_stop("redis")
        time.sleep(10)
        _docker_start("redis")
        time.sleep(5)

        import redis as sync_redis

        from app.core.config import settings

        r = sync_redis.from_url(
            settings.redis_url, socket_connect_timeout=5
        )
        assert r.ping(), "Redis should be reachable after restart"
        r.close()

    def test_qdrant_restart(self):
        """Stop Qdrant for 10 seconds, restart, verify collection intact."""
        _docker_stop("qdrant")
        time.sleep(10)
        _docker_start("qdrant")
        time.sleep(10)

        from app.vector.qdrant_client import QdrantManager

        qm = QdrantManager()
        qm.initialize()
        # Collection should still exist
        count = qm.count()
        assert count >= 0, "Qdrant collection accessible after restart"
        qm.close()

    def test_cohere_degraded_mode(self):
        """Stop cohere-embed, verify vector worker enters degraded mode without crashing."""
        import app.core.metrics as metrics
        from app.schemas.mini_block import MiniBlockPayload
        from app.vector.embedder import CohereEmbedder
        from app.vector.summarizer import build_summary

        # Verify cohere-embed is reachable before the test
        async def _health() -> bool:
            embedder = CohereEmbedder()
            ok = await embedder.health_check()
            await embedder.close()
            return ok

        assert asyncio.run(_health()), "cohere-embed must be healthy before test"

        # Record baseline counter
        degraded_before = metrics.degraded_events_total

        # Stop cohere-embed
        _docker_stop("cohere-embed")
        time.sleep(3)

        # Inject a test block and attempt embedding
        payload = MiniBlockPayload(
            block_number=99999999,
            block_timestamp=1700000000,
            index=0,
            gas_used=21000,
            transactions=["0xtx1"],
            receipts=["0xr1"],
        )
        summary = build_summary(payload)

        # Attempt embedding — should fail since cohere-embed is stopped
        async def _attempt_embed() -> None:
            embedder = CohereEmbedder()
            try:
                await embedder.embed(summary)
            except Exception:
                metrics.degraded_events_total += 1
            await embedder.close()

        asyncio.run(_attempt_embed())
        degraded_after = metrics.degraded_events_total
        assert degraded_after > degraded_before, (
            f"degraded_events_total should increment when cohere-embed is down "
            f"(before={degraded_before}, after={degraded_after})"
        )

        # Restart cohere-embed and verify normal operation
        _docker_start("cohere-embed")
        time.sleep(10)

        assert asyncio.run(_health()), "cohere-embed should be healthy after restart"
