"""Supervisor entrypoint — runs all 4 async tasks concurrently.

This is the single entrypoint for the Docker container.
It starts:
1. FastAPI server (health + metrics)
2. Ingestion daemon (WebSocket -> Redis)
3. State tracker worker (Redis -> PostgreSQL)
4. Vector indexer worker (Redis -> Qdrant)
"""

from __future__ import annotations

import asyncio
import signal

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

    from app.api.routes.health import router as health_router
    from app.api.routes.metrics import router as metrics_router

    app.include_router(health_router)
    app.include_router(metrics_router)

    return app


async def supervise() -> None:
    """Supervisor — runs all 4 async tasks and the API server concurrently."""
    from app.ingestion.daemon import main as ingestion_main
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

    # Start all 4 tasks concurrently
    api_task = asyncio.create_task(server.serve())
    ingestion_task = asyncio.create_task(ingestion_main())
    state_task = asyncio.create_task(state_worker_main())
    vector_task = asyncio.create_task(vector_worker_main())

    tasks = [api_task, ingestion_task, state_task, vector_task]

    # Wait for shutdown signal
    await shutdown_event.wait()

    logger.info("supervisor shutting down all tasks")

    # Cancel all tasks
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("supervisor shutdown complete")


def main() -> None:
    """Entrypoint called from Docker CMD."""
    asyncio.run(supervise())


if __name__ == "__main__":
    main()
