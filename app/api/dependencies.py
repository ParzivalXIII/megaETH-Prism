"""Shared FastAPI dependencies for Phase 2 + Phase 3.

Provides dependency-injection callables for database sessions, Redis client,
execution queue, worker pool, and the agent graph.

All cached dependencies use module-level singletons to avoid re-initialisation
on every request.
"""

from __future__ import annotations

from typing import Any

import redis.asyncio as aioredis
from langchain_openai import ChatOpenAI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.agents.dispatch import ExecutionQueue
from app.agents.dispatcher import WorkerPool
from app.agents.graph import build_agent_graph
from app.agents.system_prompt import AGENT_SYSTEM_PROMPT
from app.agents.tools import (
    dispatch_execution,
    query_balance,
    query_historical_subgraph,
    query_portfolio,
    search_cognitive_memory,
)
from app.core.config import settings
from app.core.logging import get_logger
from app.state.database import create_async_engine_pool

logger = get_logger("megaeth.api.dependencies")

# ---------------------------------------------------------------------------
# Cached singletons
# ---------------------------------------------------------------------------

_engine = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_redis_client: aioredis.Redis | None = None
_execution_queue: ExecutionQueue | None = None
_worker_pool: WorkerPool | None = None
_agent_graph: Any = None
_subgraph_client: Any = None
_qdrant_manager: Any = None
_embedder: Any = None


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


async def get_agent_graph() -> Any:
    """Return the compiled LangGraph agent graph (singleton).

    Validates ``OPENCODE_GO_API_KEY`` on first call (fail fast).  Creates
    a ``ChatOpenAI`` instance, collects all tools, and builds the graph
    using ``build_agent_graph()``.

    The singleton is cached — subsequent calls return the same compiled
    graph, which is thread-safe for concurrent invocations.
    """
    global _agent_graph
    global _subgraph_client
    global _qdrant_manager
    global _embedder

    if _agent_graph is not None:
        return _agent_graph

    # Validate API key (graceful degradation)
    if not settings.opencode_go_api_key:
        logger.warning("OPENCODE_GO_API_KEY not set — agent graph disabled")
        return None

    # Create LLM
    llm = ChatOpenAI(
        openai_api_base=settings.llm_base_url,
        model=settings.llm_model,
        api_key=settings.opencode_go_api_key,
        temperature=settings.llm_temperature,
        max_tokens=settings.llm_max_tokens,
    )

    # Collect tools
    tools = [
        query_historical_subgraph,
        search_cognitive_memory,
        query_portfolio,
        query_balance,
        dispatch_execution,
    ]

    # Build graph
    _agent_graph = build_agent_graph(
        llm=llm,
        tools=tools,
        system_prompt=AGENT_SYSTEM_PROMPT,
    )

    # Build runtime clients (used by AgentRunner context)
    _subgraph_client = None  # Created per-invocation from settings
    _qdrant_manager = None  # Created per-invocation
    _embedder = None  # Created per-invocation

    logger.info("agent graph initialised", model=settings.llm_model)
    return _agent_graph
