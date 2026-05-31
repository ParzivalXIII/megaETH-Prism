"""Redis Stream field-name constants and StreamEntry model.

These constants are the **canonical** Redis Stream field names used across
the entire pipeline. Every producer and consumer MUST use these constants
rather than bare string literals to prevent drift.
"""

from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Redis Stream field-name constants
# ---------------------------------------------------------------------------

STREAM_FIELD_BLOCK_NUMBER = "block_number"
"""Redis Stream field: the block number (int)."""

STREAM_FIELD_RAW_PAYLOAD = "raw_payload"
"""Redis Stream field: JSON-encoded ``MiniBlockPayload``."""

STREAM_FIELD_INGESTED_AT = "ingested_at"
"""Redis Stream field: Unix timestamp (``time.time()``) when the entry was
first written to the stream."""

STREAM_FIELD_CORRELATION_ID = "correlation_id"
"""Redis Stream field: opaque correlation/request ID for tracing."""


# ---------------------------------------------------------------------------
# Parsed entry model
# ---------------------------------------------------------------------------

class StreamEntry(BaseModel):
    """A single parsed Redis stream entry.

    This is the **output** of the stream consumer after it reads raw field
    values from Redis and deserialises the JSON payload.  It is **not**
    written to Redis directly (the raw fields are written separately).
    """

    block_number: int
    raw_payload: str  # JSON string of MiniBlockPayload
    ingested_at: float  # time.time()
    correlation_id: str

    model_config = ConfigDict(frozen=True, extra="forbid")
