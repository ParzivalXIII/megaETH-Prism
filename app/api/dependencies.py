"""Shared FastAPI dependencies.

Placeholder module for dependency-injection callables used by
Phase 2 API routes (e.g., ``get_db_session``, ``get_current_user``,
``get_redis_client``).

This file is intentionally empty in Phase 1 — dependency logic is
currently inlined in the health-check routes.  Populate it when
Phase 2 adds authenticated or transactional endpoints.
"""

# Phase 2 will add imports like:
#   from fastapi import Depends, HTTPException
#   from sqlalchemy.ext.asyncio import AsyncSession
#   from app.state.database import create_async_engine_pool
