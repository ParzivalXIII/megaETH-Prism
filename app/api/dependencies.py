"""Shared FastAPI dependencies for Phase 2.

Provides dependency-injection callables for database sessions, Redis client,
execution queue, worker pool, and the agent graph placeholder.

All cached dependencies use module-level singletons to avoid re-initialisation
on every request.
"""

from __future__ import annotations

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.agents.dispatch import ExecutionQueue
from app.agents.dispatcher import WorkerPool
from app.core.config import settings
from app.state.database import create_async_engine_pool

# ---------------------------------------------------------------------------
# Cached singletons
# ---------------------------------------------------------------------------

_engine = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_redis_client: aioredis.Redis | None = None
_execution_queue: ExecutionQueue | None = None
_worker_pool: WorkerPool | None = None
_agent_graph = None


async def get_db_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the async session factory (singleton, cached after first call).

    Creates the engine pool on first call; reuses the same factory thereafter.
    """
    global _engine, _session_factory
    if _session_factory is None:
        _engine, _session_factory = create_async_engine_pool()  # type: ignore[no-untyped-call]
    return _session_factory


async def get_redis_client() -> aioredis.Redis:
    """Return a shared async Redis client (singleton).

    Configures ``decode_responses=True`` for string-based stream operations.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(  # type: ignore[no-untyped-call]
            settings.redis_url, decode_responses=True
        )
        await _redis_client.ping()
    return _redis_client


async def get_execution_queue() -> ExecutionQueue:
    """Return an initialised ExecutionQueue (singleton).

    The queue is created once and reused across the application lifecycle.
    """
    global _execution_queue
    if _execution_queue is None:
        redis_client = await get_redis_client()
        _execution_queue = ExecutionQueue(redis_client=redis_client)
        await _execution_queue.initialize()
    return _execution_queue


async def get_worker_pool() -> WorkerPool:
    """Return a WorkerPool instance.

    The pool is **not started** by this dependency — the caller manages
    lifecycle via ``pool.start()`` and ``pool.stop()``.
    """
    global _worker_pool
    if _worker_pool is None:
        _worker_pool = WorkerPool()
    return _worker_pool


async def get_agent_graph() -> None:
    """Placeholder returning ``None``.

    Phase 3 will return a compiled LangGraph agent graph.  This function
    is reserved and documented for that purpose — routes that depend on it
    should handle ``None`` gracefully.
    """
    global _agent_graph
    return _agent_graph
