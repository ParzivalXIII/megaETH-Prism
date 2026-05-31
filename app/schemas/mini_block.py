"""MiniBlockPayload — canonical data contract for MegaETH mini-block RPC payloads.

This model matches the ACTUAL MegaETH testnet miniBlock subscription response
as observed from wss://carrot.megaeth.com/ws:
- Fields are hex-encoded strings from the wire
- Converted to native Python types via model_validator
- Field names match the actual API (not outdated docs)

Design decisions:
- Accepts both hex strings and native integers on input
- Stores converted native types internally
- Frozen (immutable) after construction for safe concurrent access
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


def _hex_or_int(v: Any) -> int:
    """Convert hex string or int to int."""
    if isinstance(v, str):
        return int(v, 16)
    if isinstance(v, int):
        return v
    raise TypeError(f"Expected hex string or int, got {type(v).__name__}: {v}")


class MiniBlockPayload(BaseModel):
    """Represents a single mini-block as received from MegaETH RPC subscription.

    The API returns hex-encoded strings. This model accepts both hex strings
    and native integers on input, storing native Python ints internally.
    """

    block_number: int
    block_timestamp: int  # seconds epoch
    index: int
    gas_used: int
    transactions: list[Any]  # API returns full tx objects or empty list
    receipts: list[Any]      # API returns full receipt objects or empty list
    # Optional fields present in the API response
    mini_block_number: int | None = None
    mini_block_timestamp: int | None = None  # microseconds epoch
    transaction_root: str | None = None
    receipt_root: str | None = None

    model_config = ConfigDict(frozen=True, extra="ignore")

    @field_validator(
        "block_number",
        "block_timestamp",
        "index",
        "gas_used",
        "mini_block_number",
        "mini_block_timestamp",
        mode="before",
    )
    @classmethod
    def _convert_hex_int(cls, v: Any) -> int | None:
        """Accept both hex strings and integers for numeric fields."""
        if v is None:
            return None
        return _hex_or_int(v)

    @property
    def tx_count(self) -> int:
        """Derived property: number of transactions in this mini-block."""
        return len(self.transactions)
