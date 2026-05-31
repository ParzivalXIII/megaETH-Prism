"""Simple module-level counters for telemetry.

These counters are shared across all async tasks in the same process.
They will be replaced by a proper Prometheus/metrics system in T9.
"""

# ---------------------------------------------------------------------------
# Ingestion daemon counters
# ---------------------------------------------------------------------------

blocks_ingested_total: int = 0
"""Number of mini-blocks ingested and pushed to the Redis stream."""

ws_reconnects_total: int = 0
"""Number of times the WebSocket connection has been re-established."""

ws_keepalive_sent_total: int = 0
"""Number of keepalive eth_chainId requests sent."""

ws_keepalive_errors_total: int = 0
"""Number of keepalive attempts that failed."""

ws_subscription_errors_total: int = 0
"""Number of malformed or non-subscription messages encountered."""

# ---------------------------------------------------------------------------
# Stream processor counters (placeholders, populated by T8)
# ---------------------------------------------------------------------------

stream_reads_total: int = 0
stream_read_errors_total: int = 0

# ---------------------------------------------------------------------------
# State tracker counters (populated by T7)
# ---------------------------------------------------------------------------

blocks_processed_total: int = 0
"""Number of mini-blocks successfully upserted into PostgreSQL."""

blocks_failed_total: int = 0
"""Number of mini-blocks that failed during state tracking."""

# ---------------------------------------------------------------------------
# Embedding / vector counters
# ---------------------------------------------------------------------------

vectors_upserted_total: int = 0
"""Number of vectors successfully upserted to Qdrant."""

vectors_search_total: int = 0
"""Number of similarity searches performed against Qdrant."""

embeddings_cached: int = 0
"""Number of embedding cache hits (duplicate summaries)."""

embeddings_computed: int = 0
"""Number of embedding API calls made (actual compute)."""

degraded_events_total: int = 0
"""Number of blocks processed in degraded mode (embedding unavailable)."""


__all__ = [
    "blocks_ingested_total",
    "ws_reconnects_total",
    "ws_keepalive_sent_total",
    "ws_keepalive_errors_total",
    "ws_subscription_errors_total",
    "stream_reads_total",
    "stream_read_errors_total",
    "blocks_processed_total",
    "blocks_failed_total",
    "vectors_upserted_total",
    "vectors_search_total",
    "embeddings_cached",
    "embeddings_computed",
    "degraded_events_total",
]
