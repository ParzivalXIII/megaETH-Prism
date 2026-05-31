"""VectorPayload — Qdrant point descriptor for mini-block embeddings.

Each mini-block that is ingested may be summarised and embedded for
semantic search.  The ``VectorPayload`` model describes the data stored
in a single Qdrant point.

Design:
- ``payload_id`` is the **RPC hex string**, not a generated UUID — this
  ensures traceability back to the on-chain payload.
- ``embedding`` is a 1024-dimensional ``list[float]`` (default Qdrant
  vector size for the pipeline).
- ``summary`` is capped at 500 characters.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class VectorPayload(BaseModel):
    """Descriptor for a single Qdrant point representing a mini-block.

    Attributes
    ----------
    payload_id : str
        RPC ``payload_id`` (hex string), **not** a generated UUID.
    block_number : int
        The parent block number.
    summary : str
        Text summary of the mini-block (max 500 chars).
    embedding : list[float]
        1024-dimensional embedding vector.
    metadata : dict
        Arbitrary key-value metadata.
    """

    payload_id: str
    block_number: int
    summary: str = Field(max_length=500)
    embedding: list[float]
    metadata: dict = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("embedding")
    @classmethod
    def _validate_embedding_dimension(cls, v: list[float]) -> list[float]:
        """Ensure the embedding vector is exactly 1024-dimensional."""
        if len(v) != 1024:
            raise ValueError(
                f"Embedding must be 1024-dimensional, got {len(v)}"
            )
        return v
