"""Unit tests for the ExecutionPayload and TriggerCondition schemas.

Covers:
- Happy-path validation with defaults
- Slippage cap enforcement (≤100 bps)
- Missing required fields
- Extra field rejection (extra="forbid")
- Invalid call_data and target_contract formats
- Invalid priority and condition_type values
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from app.schemas.intent import ExecutionPayload, TriggerCondition


class TestTriggerCondition:
    def test_valid_always(self) -> None:
        tc = TriggerCondition(condition_type="always")
        assert tc.condition_type == "always"
        assert tc.params == {}

    def test_valid_with_params(self) -> None:
        tc = TriggerCondition(
            condition_type="price_threshold",
            params={"token": "0xabc", "threshold": "1.50"},
        )
        assert tc.condition_type == "price_threshold"
        assert tc.params["token"] == "0xabc"

    def test_invalid_condition_type(self) -> None:
        with pytest.raises(ValidationError):
            TriggerCondition(condition_type="unknown")

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TriggerCondition(condition_type="always", bogus="field")


class TestExecutionPayload:
    @pytest.fixture
    def valid_payload(self) -> dict[str, Any]:
        return {
            "target_contract": "0x402085c248EeA27D92E8b30b2C58ed07f9E20001",
            "call_data": "0xdeadbeef",
            "trigger_condition": {"condition_type": "always"},
        }

    def test_valid_execution_payload(self, valid_payload: dict[str, Any]) -> None:
        p = ExecutionPayload.model_validate(valid_payload)
        assert p.target_contract == "0x402085c248eea27d92e8b30b2c58ed07f9e20001"  # lowercased
        assert p.call_data == "0xdeadbeef"
        assert p.max_gas_price == 1_000_000  # default
        assert p.max_slippage_bps == 100  # default
        assert p.priority == "normal"  # default
        assert p.trigger_condition.condition_type == "always"
        assert p.id is not None
        assert p.created_at > 0

    def test_slippage_cap_enforced(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "max_slippage_bps": 101}
        with pytest.raises(ValidationError) as excinfo:
            ExecutionPayload.model_validate(data)
        assert "slippage" in str(excinfo.value).lower()

    def test_slippage_zero_valid(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "max_slippage_bps": 0}
        p = ExecutionPayload.model_validate(data)
        assert p.max_slippage_bps == 0

    def test_missing_required_field(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            ExecutionPayload.model_validate({"call_data": "0xaa"})
        assert "target_contract" in str(excinfo.value)

    def test_extra_field_rejected(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "bogus": "field"}
        with pytest.raises(ValidationError):
            ExecutionPayload.model_validate(data)

    def test_invalid_call_data_no_prefix(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "call_data": "deadbeef"}
        with pytest.raises(ValidationError) as excinfo:
            ExecutionPayload.model_validate(data)
        assert "0x" in str(excinfo.value)

    def test_invalid_call_data_non_hex(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "call_data": "0xzzzz"}
        with pytest.raises(ValidationError):
            ExecutionPayload.model_validate(data)

    def test_invalid_target_contract_length(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "target_contract": "0x123"}
        with pytest.raises(ValidationError) as excinfo:
            ExecutionPayload.model_validate(data)
        assert "42" in str(excinfo.value)

    def test_invalid_target_contract_no_prefix(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "target_contract": "dead" * 10}
        with pytest.raises(ValidationError) as excinfo:
            ExecutionPayload.model_validate(data)
        assert "0x" in str(excinfo.value)

    def test_invalid_priority(self, valid_payload: dict[str, Any]) -> None:
        data = {**valid_payload, "priority": "urgent"}
        with pytest.raises(ValidationError):
            ExecutionPayload.model_validate(data)

    def test_default_values(self) -> None:
        p = ExecutionPayload(
            target_contract="0x402085c248EeA27D92E8b30b2C58ed07f9E20001",
            call_data="0xdeadbeef",
            trigger_condition=TriggerCondition(condition_type="always"),
        )
        assert p.max_gas_price == 1_000_000
        assert p.max_slippage_bps == 100
        assert p.priority == "normal"
        assert p.created_by_agent == ""
