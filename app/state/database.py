"""Dual engine setup for PostgreSQL operations.

Uses a synchronous engine for DDL (create_all, schema migrations) and an
async engine with connection pooling for runtime operations.  The sync
engine derives its URL by stripping the ``+asyncpg`` driver suffix.
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings


def create_sync_engine():
    """Create a synchronous engine for DDL operations (create_all, health checks).

    Strips ``+asyncpg`` from the URL to get the sync driver URL.
    """
    sync_url = settings.postgres_url.replace("+asyncpg", "")
    return create_engine(sync_url, pool_pre_ping=True)


def create_async_engine_pool():
    """Create an async engine with connection pooling for runtime operations.

    Returns
    -------
    tuple[sqlalchemy.ext.asyncio.AsyncEngine, async_sessionmaker[AsyncSession]]
        The async engine and a session factory configured with
        ``expire_on_commit=False``.
    """
    engine = create_async_engine(
        settings.postgres_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return engine, session_factory
