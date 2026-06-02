"""AssetBalance & TokenTransfer — SQLModel table definitions for asset state tracking.

These models store real-time user asset balances derived from ERC-20 Transfer
event extraction.  The ``AssetBalance`` table is the materialised view updated
by the balance tracker worker.  ``TokenTransfer`` stores raw event data.

Design decisions:
- Composite PK ``(user_address, token_address)`` for idempotent upsert.
- All addresses normalised to lowercase 0x-prefixed.
- ``balance_raw`` is the raw uint256 value; ``balance_human`` property
  divides by 10^decimals.
- ``TokenTransfer`` uses auto-increment ``id`` (surrogate PK) because
  ``(block_number, tx_index, log_index)`` could collide across reorgs.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import ConfigDict
from sqlalchemy import Column, DateTime, func
from sqlmodel import Field, SQLModel


class AssetBalance(SQLModel, table=True):
    """Per-user asset balance tracked from on-chain ERC-20 Transfer events.

    Upserted idempotently via ``(user_address, token_address)`` composite PK.
    Only contains balances derived from Transfer events — no on-chain
    ``balanceOf`` bootstrap (delta-only tracking).
    """

    __tablename__: str = "asset_balances"  # type: ignore[misc]

    user_address: str = Field(
        primary_key=True,
        max_length=42,
        description="0x-prefixed user address (lowercase)",
    )
    token_address: str = Field(
        primary_key=True,
        max_length=42,
        description="ERC-20 contract address (lowercase)",
    )
    token_symbol: str = Field(
        default="",
        max_length=20,
        index=True,
        description="Short token symbol (e.g. USDM, WETH)",
    )
    balance_raw: int = Field(
        default=0,
        description="Raw balance (no decimals applied, uint256)",
    )
    decimals: int = Field(
        default=18,
        description="Token decimals (for human-readable conversion)",
    )
    block_number: int = Field(
        default=0,
        description="Last block number that updated this row",
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime(timezone=True), server_default=func.now()),
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(
            DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now(),
        ),
    )

    @property
    def balance_human(self) -> float:
        """Human-readable balance (raw / 10^decimals)."""
        if self.decimals == 0:
            return float(self.balance_raw)
        return float(self.balance_raw) / float(10**self.decimals)

    model_config = ConfigDict(populate_by_name=True)  # type: ignore[assignment]


class TokenTransfer(SQLModel, table=True):
    """Raw ERC-20 Transfer event log extracted from mini-block receipts.

    This is the denormalised raw event store.  ``AssetBalance`` is the
    materialised aggregation target.
    """

    __tablename__: str = "token_transfers"  # type: ignore[misc]

    id: int | None = Field(default=None, primary_key=True)
    """Auto-increment surrogate primary key."""

    block_number: int = Field(
        default=0,
        index=True,
        description="Block containing this transfer",
    )
    tx_index: int = Field(
        default=0,
        description="Transaction index within the block",
    )
    log_index: int = Field(
        default=0,
        description="Log index within the receipt",
    )
    token_address: str = Field(
        max_length=42,
        index=True,
        description="ERC-20 contract address (lowercase)",
    )
    from_address: str = Field(
        max_length=42,
        index=True,
        description="Sender address (lowercase)",
    )
    to_address: str = Field(
        max_length=42,
        index=True,
        description="Recipient address (lowercase)",
    )
    amount: int = Field(
        default=0,
        description="Transfer amount (uint256 raw, no decimals)",
    )
    ingested_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(DateTime(timezone=True), server_default=func.now()),
    )

    model_config = ConfigDict(populate_by_name=True)  # type: ignore[assignment]
