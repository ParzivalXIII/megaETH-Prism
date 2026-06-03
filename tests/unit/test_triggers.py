"""Unit tests for trigger condition evaluators.

Covers:
1. "always" condition always returns True
2. "price_threshold" evaluates correctly
3. "time_bound" evaluates correctly
4. "price_threshold" with unknown direction returns False
"""

from __future__ import annotations

import time

import pytest

from app.schemas.intent import TriggerCondition


class TestTriggerEvaluators:
    """Test trigger condition evaluators via the dispatcher's _check_conditions.

    We import from app.agents.dispatcher and test the executor directly.
    """

    @pytest.fixture
    def executor(self) -> object:
        from app.agents.dispatcher import TransactionExecutor

        return TransactionExecutor()

    @pytest.mark.asyncio
    async def test_always_returns_true(self, executor: object) -> None:
        """'always' condition type should always return True."""
        condition = TriggerCondition(condition_type="always")
        result = await executor._check_conditions(condition)
        assert result is True

    @pytest.mark.asyncio
    async def test_price_threshold_above(self, executor: object) -> None:
        """Price threshold 'above' — current ETH price (3000) >= threshold (2500)."""
        condition = TriggerCondition(
            condition_type="price_threshold",
            params={"token_pair": "ETH/USD", "threshold": 2500.0, "direction": "above"},
        )
        result = await executor._check_conditions(condition)
        assert result is True

    @pytest.mark.asyncio
    async def test_price_threshold_below_not_met(self, executor: object) -> None:
        """Price threshold 'below' — current ETH price (3000) <= threshold (2000) is False."""
        condition = TriggerCondition(
            condition_type="price_threshold",
            params={"token_pair": "ETH/USD", "threshold": 2000.0, "direction": "below"},
        )
        result = await executor._check_conditions(condition)
        assert result is False

    @pytest.mark.asyncio
    async def test_time_bound_deferred(self, executor: object) -> None:
        """Time-bound trigger defers when execute_at is in the future."""
        future_ts = time.time() + 3600  # 1 hour from now
        condition = TriggerCondition(
            condition_type="time_bound",
            params={"execute_at": future_ts, "before": future_ts + 7200},
        )
        result = await executor._check_conditions(condition)
        assert result is False

    @pytest.mark.asyncio
    async def test_time_bound_met(self, executor: object) -> None:
        """Time-bound trigger returns True when within window."""
        past_ts = time.time() - 3600  # 1 hour ago
        future_ts = time.time() + 3600  # 1 hour from now
        condition = TriggerCondition(
            condition_type="time_bound",
            params={"execute_at": past_ts, "before": future_ts},
        )
        result = await executor._check_conditions(condition)
        assert result is True

    @pytest.mark.asyncio
    async def test_unknown_condition_type(self, executor: object) -> None:
        """Unknown condition type returns False."""
        # We can't create a condition with unknown type (Literal restricts),
        # so we check that price_threshold with unknown direction returns False
        condition = TriggerCondition(
            condition_type="price_threshold",
            params={"token_pair": "ETH/USD", "threshold": 2500.0, "direction": "unknown"},
        )
        result = await executor._check_conditions(condition)
        assert result is False
