"""ExecutionPayload & TriggerCondition — Pydantic schemas for agent execution intents.

These models define the JSON contract between future LangGraph agents and the
async worker pool that dispatches transactions to MegaETH.  Every execution
plan that flows through the Redis Stream queue is represented as an
``ExecutionPayload`` with a ``TriggerCondition`` describing when to execute.

Design decisions:
- Frozen models for safe concurrent access across asyncio tasks.
- ``extra="forbid"`` to reject unknown fields (security boundary).
- Max slippage capped at 100 bps (1 %) at schema level — this is a hard
  safety constraint that cannot be bypassed.
- ``call_data`` is raw ABI-encoded hex (the worker signs, not encodes).
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TriggerCondition(BaseModel):
    """Describes the conditions under which an execution plan should fire.

    Phase 2 only evaluates ``condition_type="always"``.  The ``price_threshold``
    and ``time_bound`` schemas exist for forward compatibility with Phase 3
    oracle integration — evaluation returns ``False`` with a WARNING log.
    """

    condition_type: Literal["always", "price_threshold", "time_bound"]
    """Which trigger evaluator to use."""

    params: dict[str, Any] = Field(default_factory=dict)
    """Arbitrary parameters for the trigger evaluator (e.g. threshold price)."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class ExecutionPayload(BaseModel):
    """Deterministic execution definition written by agents, consumed by workers.

    Agents produce JSON conforming to this schema.  Workers consume it,
    validate conditions, assemble and broadcast transactions via the
    async worker pool.

    Safety constraints enforced at schema level:
    - ``max_slippage_bps`` cannot exceed 100 (1 %).
    - ``target_contract`` must be a valid 0x-prefixed 42-char address.
    - ``call_data`` must be 0x-prefixed hex.
    - Unknown fields raise ``ValidationError``.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """Auto-generated unique execution ID."""

    target_contract: str
    """The contract address to call (0x-prefixed, 42 characters)."""

    call_data: str
    """ABI-encoded calldata (0x-prefixed hex)."""

    max_gas_price: int = Field(default=1_000_000)
    """Maximum gas price in wei (MegaETH base fee is ~0.001 gwei)."""

    trigger_condition: TriggerCondition
    """When this execution plan should fire."""

    max_slippage_bps: int = Field(default=100, ge=0, le=100)
    """Maximum acceptable slippage in basis points (1 % = 100 bps, hard cap)."""

    priority: Literal["normal", "high", "low"] = Field(default="normal")
    """Relative priority for the worker pool."""

    created_by_agent: str = Field(default="")
    """Name of the agent that created this payload (for auditing)."""

    created_at: float = Field(default_factory=time.time)
    """Unix timestamp of creation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("target_contract")
    @classmethod
    def _validate_address(cls, v: str) -> str:
        """Validate and normalize contract address."""
        if not isinstance(v, str):
            raise ValueError("target_contract must be a string")
        v = v.lower()
        if not v.startswith("0x"):
            raise ValueError("target_contract must start with 0x")
        if len(v) != 42:
            raise ValueError(
                f"target_contract must be 42 characters (0x + 40 hex digits), got {len(v)}"
            )
        try:
            int(v, 16)
        except ValueError:
            raise ValueError("target_contract must be a valid hex address")
        return v

    @field_validator("call_data")
    @classmethod
    def _validate_call_data(cls, v: str) -> str:
        """Validate that call_data is 0x-prefixed hex."""
        if not isinstance(v, str):
            raise ValueError("call_data must be a string")
        if not v.startswith("0x"):
            raise ValueError("call_data must start with 0x")
        try:
            int(v, 16)
        except ValueError:
            raise ValueError("call_data must be valid hex")
        return v
