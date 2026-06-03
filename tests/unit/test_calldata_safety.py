"""Unit tests for the calldata safety layer in dispatch_execution.

Covers:
1. Safe transfer (0xa9059cbb) accepted
2. Approve (0x095ea7b3) accepted when in allowlist
3. Dangerous transferFrom (0x23b872dd) blocked
4. Unknown selector blocked
5. Wildcard bypass (* allows all)
6. Slippage cap enforced (≤100 bps)
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.agents.tools.dispatch import (
    _check_calldata,
)


class TestCalldataSafety:
    @pytest.fixture(autouse=True)
    def _setup_settings(self) -> None:
        """Ensure settings has the default allowlist."""
        from app.core.config import settings

        settings.agent_calldata_allowlist_csv = "0xa9059cbb,0x095ea7b3,0x38ed1739"

    def test_safe_transfer_accepted(self) -> None:
        """Transfer selector 0xa9059cbb should be allowed."""
        calldata = "0xa9059cbb0000000000000000000000000000000000000000000000000000000000000001"
        allowed, reason = _check_calldata(calldata)
        assert allowed, f"Safe transfer should be allowed, got: {reason}"
        assert "allowlist" in reason

    def test_safe_approve_allowed(self) -> None:
        """Approve selector 0x095ea7b3 should be allowed (in default allowlist)."""
        calldata = "0x095ea7b30000000000000000000000000000000000000000000000000000000000000001"
        allowed, reason = _check_calldata(calldata)
        assert allowed, f"Approve should be allowed (in allowlist), got: {reason}"

    def test_dangerous_transferfrom_blocked(self) -> None:
        """transferFrom selector 0x23b872dd should be blocked."""
        calldata = "0x23b872dd0000000000000000000000000000000000000000000000000000000000000001"
        allowed, reason = _check_calldata(calldata)
        assert not allowed, f"transferFrom should be blocked, got: {reason}"
        assert "blocked" in reason or "dangerous" in reason

    def test_unknown_selector_blocked(self) -> None:
        """Unknown selector not in allowlist should be blocked."""
        calldata = "0xdeadbeef00000000000000000000000000000000000000000000000000000000"
        allowed, reason = _check_calldata(calldata)
        assert not allowed, f"Unknown selector should be blocked, got: {reason}"
        assert "not in the allowlist" in reason

    def test_wildcard_bypass(self) -> None:
        """Wildcard '*' allowlist should allow any selector."""
        from app.core.config import settings

        settings.agent_calldata_allowlist_csv = "*"
        calldata = "0x23b872dd0000000000000000000000000000000000000000000000000000000000000001"
        allowed, reason = _check_calldata(calldata)
        assert allowed, f"Wildcard should allow all, got: {reason}"
        assert "wildcard" in reason

    @pytest.mark.asyncio
    async def test_slippage_cap_enforced(self) -> None:
        """Slippage > 100 bps should be rejected by dispatch_execution."""
        # Test at schema level
        import pydantic

        from app.schemas.intent import ExecutionPayload

        with pytest.raises(pydantic.ValidationError) as excinfo:
            ExecutionPayload(
                target_contract="0x402085c248EeA27D92E8b30b2C58ed07f9E20001",
                call_data="0xa9059cbb0000000000000000000000000000000000000000000000000000000000000001",
                trigger_condition={"condition_type": "always"},
                max_slippage_bps=101,
            )
        assert "slippage" in str(excinfo.value).lower() or "max_slippage_bps" in str(excinfo.value).lower()

        # Also test the tool-level check
        # Call the underlying function directly to avoid LangChain StructuredTool wrapping
        from app.agents.tools.dispatch import dispatch_execution as dispatch_fn

        # Access the async function directly
        result = await dispatch_fn.coroutine(
            payload={
                "target_contract": "0x402085c248EeA27D92E8b30b2C58ed07f9E20001",
                "call_data": "0xa9059cbb0000000000000000000000000000000000000000000000000000000000000001",
                "trigger_condition": {"condition_type": "always"},
                "max_slippage_bps": 200,
            },
            config={"configurable": {"execution_queue": AsyncMock()}},
        )
        assert result["status"] == "rejected"
        assert "slippage" in result["reason"].lower()
