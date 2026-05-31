import time

from fastapi import APIRouter
from starlette.responses import PlainTextResponse

import app.core.metrics as m

router = APIRouter(tags=["metrics"])

_start_time = time.monotonic()

METRIC_TEMPLATE = "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"

METRICS = [
    (
        "megaeth_blocks_ingested_total",
        "Total mini-blocks ingested from WebSocket",
        lambda: m.blocks_ingested_total,
    ),
    (
        "megaeth_ws_reconnects_total",
        "WebSocket reconnections",
        lambda: m.ws_reconnects_total,
    ),
    (
        "megaeth_blocks_processed_total",
        "Blocks upserted to PostgreSQL",
        lambda: m.blocks_processed_total,
    ),
    (
        "megaeth_blocks_failed_total",
        "Block processing failures",
        lambda: m.blocks_failed_total,
    ),
    (
        "megaeth_vectors_upserted_total",
        "Vectors upserted to Qdrant",
        lambda: m.vectors_upserted_total,
    ),
    (
        "megaeth_embeddings_cached_total",
        "Embedding cache hits",
        lambda: m.embeddings_cached,
    ),
    (
        "megaeth_embeddings_computed_total",
        "Embedding API calls",
        lambda: m.embeddings_computed,
    ),
    (
        "megaeth_degraded_events_total",
        "Degraded mode events (embedding unavailable)",
        lambda: m.degraded_events_total,
    ),
]

GAUGE_METRICS = [
    (
        "megaeth_uptime_seconds",
        "Application uptime in seconds",
        lambda: round(time.monotonic() - _start_time, 2),
    ),
]


@router.get("/metrics")
async def metrics():
    """Prometheus-compatible metrics endpoint.

    Returns plain text in Prometheus exposition format.
    All counters are module-level and shared across all async tasks.
    """
    lines: list[str] = []
    for name, help_text, value_fn in METRICS:
        val = value_fn()
        lines.append(METRIC_TEMPLATE.format(name=name, help=help_text, value=val))

    for name, help_text, value_fn in GAUGE_METRICS:
        lines.append(
            f"# HELP {name} {help_text}\n# TYPE {name} gauge\n{name} {value_fn()}\n"
        )

    return PlainTextResponse("".join(lines))
