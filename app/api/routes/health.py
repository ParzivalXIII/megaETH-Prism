import time

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine
from sqlalchemy import text as sa_text

from app.core.config import settings
from app.core.logging import get_logger
from app.vector.qdrant_client import QdrantManager

router = APIRouter(tags=["health"])

_start_time = time.monotonic()

logger = get_logger("megaeth.api.health")


@router.get("/health/live")
async def health_live() -> dict[str, str]:
    """Liveness check — always returns 200 if the process is alive."""
    return {"status": "ok"}


@router.get("/health/ready")
async def health_ready() -> JSONResponse:
    """Readiness check — verifies all downstream dependencies are reachable.

    Returns 200 if all are healthy, 503 if any dependency is down.
    Each dependency returns "healthy" or an error message.
    """

    checks: dict[str, str] = {}
    overall_status = "healthy"

    # Check PostgreSQL
    try:
        sync_url = settings.postgres_url.replace("+asyncpg", "")
        engine = create_engine(sync_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(sa_text("SELECT 1"))
        engine.dispose()
        checks["postgres"] = "healthy"
    except Exception as e:
        checks["postgres"] = f"unhealthy: {str(e)}"
        overall_status = "degraded"

    # Check Redis
    try:
        import redis as sync_redis

        r = sync_redis.from_url(settings.redis_url, socket_connect_timeout=3)
        r.ping()
        r.close()
        checks["redis"] = "healthy"
    except Exception as e:
        checks["redis"] = f"unhealthy: {str(e)}"
        overall_status = "degraded"

    # Check Qdrant
    try:
        qm = QdrantManager()
        qm.initialize()
        assert qm.COLLECTION_NAME is not None
        qm.close()
        checks["qdrant"] = "healthy"
    except Exception as e:
        checks["qdrant"] = f"unhealthy: {str(e)}"
        overall_status = "degraded"

    # Check cohere-embed
    try:
        import httpx

        resp = httpx.get(f"{settings.cohere_embed_url}/health", timeout=5.0)
        if resp.status_code == 200:
            checks["cohere_embed"] = "healthy"
        else:
            checks["cohere_embed"] = f"unhealthy: status {resp.status_code}"
            if overall_status == "healthy":
                overall_status = "degraded"
    except Exception as e:
        checks["cohere_embed"] = f"unreachable: {str(e)}"
        # cohere-embed being down is degraded, not unhealthy
        if overall_status == "healthy":
            overall_status = "degraded"

    uptime = round(time.monotonic() - _start_time, 2)

    result = {
        "status": overall_status,
        "checks": checks,
        "uptime_seconds": uptime,
    }

    status_code = 200 if overall_status == "healthy" else 503
    return JSONResponse(content=result, status_code=status_code)
