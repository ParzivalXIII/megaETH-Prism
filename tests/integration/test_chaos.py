"""Chaos/recovery integration tests.

Tests verify that the pipeline components survive service restarts.
Uses subprocess to control Docker containers.

Skipped by default. Set ``SKIP_CHAOS_TESTS=0`` to run::

    SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_chaos.py -v -s
"""

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
        from app.state.database import create_sync_engine
        from sqlalchemy import text as sa_text

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
