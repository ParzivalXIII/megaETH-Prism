"""Supervisor entrypoint — runs all 7 async tasks concurrently.

This is the single entrypoint for the Docker container.
It starts:
1. FastAPI server (health + metrics + agent API)
2. Ingestion daemon (WebSocket -> Redis)
3. State tracker worker (Redis -> PostgreSQL)
4. Vector indexer worker (Redis -> Qdrant)
5. Balance tracker worker (Redis -> PostgreSQL asset balances) [Phase 2]
6. Execution worker pool (Redis -> transaction dispatch) [Phase 2]
7. AgentRunner poll loop (LangGraph -> Redis execution queue) [Phase 3]
"""

from __future__ import annotations

import asyncio
import signal
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.logging import get_logger, setup_logging

# Set up logging first
setup_logging(settings.log_level)
logger = get_logger("megaeth.api.main")


def create_app() -> FastAPI:
    """Create the FastAPI application with routes and middleware."""
    app = FastAPI(
        title="MegaETH Orchestration System",
        version="0.1.0",
        description="Event-driven middleware for MegaETH autonomous agents",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from app.api.routes.agent import router as agent_router
    from app.api.routes.health import router as health_router
    from app.api.routes.metrics import router as metrics_router

    app.include_router(health_router)
    app.include_router(metrics_router)
    app.include_router(agent_router)

    return app


async def _build_agent_runner(
    app: FastAPI,
) -> tuple[asyncio.Task[Any], Any] | tuple[None, None]:
    """Build the AgentRunner if OPENCODE_GO_API_KEY is configured.

    Returns ``(agent_task, agent_runner)`` or ``(None, None)`` if the
    API key is not set.
    """
    if not settings.opencode_go_api_key:
        logger.info("OPENCODE_GO_API_KEY not set — skipping AgentRunner")
        return None, None

    from app.agents.runner import AgentRunner
    from app.api.dependencies import (
        get_agent_graph,
        get_db_session_factory,
        get_execution_queue,
        get_redis_client,
    )

    try:
        # Initialise dependencies
        graph = await get_agent_graph()
        session_factory = await get_db_session_factory()
        redis_client = await get_redis_client()
        execution_queue = await get_execution_queue()

        from app.vector.embedder import CohereEmbedder
        from app.vector.qdrant_client import QdrantManager

        # Build runtime context
        context = {
            "db_session_factory": session_factory,
            "redis_client": redis_client,
            "qdrant_client": QdrantManager(),
            "embedder": CohereEmbedder(),
            "execution_queue": execution_queue,
            "user_address": "",  # Set by agent at invocation time
        }

        runner = AgentRunner(
            graph=graph,
            context=context,
        )
        runner.start()
        agent_task = asyncio.create_task(runner.shutdown_event.wait())
        logger.info("Phase 3 AgentRunner started")
        return agent_task, runner

    except Exception as e:
        logger.warning(
            "AgentRunner initialisation failed — continuing without agent",
            error=str(e),
            exc_info=True,
        )
        return None, None


async def supervise() -> None:
    """Supervisor — runs all 7 async tasks and the API server concurrently."""
    from app.agents.dispatcher import WorkerPool
    from app.ingestion.daemon import main as ingestion_main
    from app.state.balance_worker import BalanceTrackerWorker
    from app.state.worker import main as state_worker_main
    from app.vector.worker import main as vector_worker_main

    # Set up shared shutdown event
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("supervisor received shutdown signal")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows or restricted environment
            pass

    # Run the API server in a separate task via uvicorn
    app = create_app()

    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=8080,
        log_level=settings.log_level.lower(),
        access_log=True,
    )
    server = uvicorn.Server(config)

    # Phase 2: Initialise workers
    worker_pool = WorkerPool(worker_count=settings.worker_pool_count)
    await worker_pool.start()
    logger.info(
        "Phase 2 worker pool started",
        worker_count=settings.worker_pool_count,
    )

    balance_worker = BalanceTrackerWorker()
    await balance_worker.start()
    logger.info("Phase 2 balance worker started")

    # Phase 3: Initialise agent runner
    agent_task, agent_runner = await _build_agent_runner(app)

    # Start all tasks concurrently
    api_task = asyncio.create_task(server.serve())
    ingestion_task = asyncio.create_task(ingestion_main())
    state_task = asyncio.create_task(state_worker_main())
    vector_task = asyncio.create_task(vector_worker_main())
    worker_shutdown_tracker = asyncio.create_task(
        worker_pool.shutdown_event.wait()
    )
    balance_shutdown_tracker = asyncio.create_task(
        balance_worker.shutdown_event.wait()
    )

    tasks = [
        api_task,
        ingestion_task,
        state_task,
        vector_task,
        worker_shutdown_tracker,
        balance_shutdown_tracker,
    ]

    # Add agent task if available
    agent_shutdown_tracker: asyncio.Task[Any] | None = None
    if agent_task is not None:
        agent_shutdown_tracker = agent_task
        tasks.append(agent_shutdown_tracker)
        logger.info("supervisor running 7 concurrent tasks")
    else:
        logger.info("supervisor running 6 concurrent tasks (no agent)")

    # Wait for shutdown signal
    await shutdown_event.wait()

    logger.info("supervisor shutting down all tasks")

    # Stop Phase 3 agent first
    if agent_runner is not None:
        await agent_runner.stop()

    # Stop Phase 2 workers
    await worker_pool.stop()
    await balance_worker.stop()

    # Cancel all remaining tasks
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("supervisor shutdown complete")


def main() -> None:
    """Entrypoint called from Docker CMD."""
    asyncio.run(supervise())


if __name__ == "__main__":
    main()
