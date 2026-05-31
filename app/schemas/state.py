"""MiniBlockRecord — PostgreSQL table model for persisted mini-block data.

This SQLModel table stores mini-blocks that have been ingested from the
MegaETH RPC subscription. ``tx_count`` is a stored column (not derived),
populated at write time from the payload's ``len(transactions)``.
"""

from datetime import datetime

from pydantic import ConfigDict
from sqlalchemy import JSON, Column, DateTime, func
from sqlmodel import Field, SQLModel


class MiniBlockRecord(SQLModel, table=True):
    """SQLModel table for persisted mini-block records.

    Fields match the actual MegaETH testnet miniBlock API response.
    Composite primary key is (block_number, index).
    ``mini_block_number`` is a unique sequential identifier for idempotency.

    Design note: The ``(block_number, index)`` composite PK supersedes the
    ``payload_id`` field from the original Phase 1 plan — ``mini_block_number``
    serves as the unique deduplication key instead.
    """

    __tablename__: str = "mini_block_records"  # type: ignore[misc]

    block_number: int = Field(primary_key=True)
    index: int = Field(primary_key=True)
    mini_block_number: int | None = Field(default=None, unique=True, nullable=True)
    block_timestamp: int = Field(default=0, description="Seconds epoch")
    gas_used: int = Field(default=0)
    tx_count: int = Field(default=0)
    raw_jsonb: dict = Field(
        default_factory=dict,
        sa_type=JSON,
        sa_column_kwargs={"server_default": "{}"},
    )
    ingested_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime(timezone=True), server_default=func.now()),
    )

    model_config = ConfigDict(populate_by_name=True)  # type: ignore[assignment]
