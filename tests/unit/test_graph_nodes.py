"""Unit tests for LangGraph node functions and routing.

Covers:
- Market intelligence node (mock LLM)
- Portfolio router node
- No-action path
- Error handling in nodes
- Routing functions
- Recursion guard

All tests use mocked LLMs — no real API calls.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from app.agents.graph import (
    _build_market_intelligence_node,
    _build_portfolio_router_node,
    _check_recursion_limit,
    _needs_tool_execution,
    _route_after_portfolio,
    _route_after_portfolio_tools,
)
from app.agents.state import AgentState


class MockLLM:
    """Mock LLM that returns predetermined responses."""

    def __init__(self, response: AIMessage | None = None):
        self._response = response or AIMessage(content="analysis complete")
        self.bind_tools_called = False

    def bind_tools(self, tools: list) -> "MockLLM":
        self.bind_tools_called = True
        return self

    async def ainvoke(self, messages: list, **kwargs: object) -> AIMessage:
        return self._response


class TestGraphNodes:
    @pytest.fixture
    def base_state(self) -> AgentState:
        return {
            "messages": [HumanMessage(content="Check my portfolio")],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        }

    @pytest.mark.asyncio
    async def test_market_intelligence_node(self, base_state: AgentState) -> None:
        """Market intelligence node processes and returns analysis."""
        mock = MockLLM()
        node = _build_market_intelligence_node(mock, [])
        result = await node(base_state)

        assert "messages" in result
        assert result["current_phase"] == "market_analysis"

    @pytest.mark.asyncio
    async def test_portfolio_router_node(self, base_state: AgentState) -> None:
        """Portfolio router node processes and returns routing decision."""
        mock = MockLLM()
        node = _build_portfolio_router_node(mock, [])
        result = await node(base_state)

        assert "messages" in result
        assert result["current_phase"] == "portfolio_routing"

    @pytest.mark.asyncio
    async def test_no_action_path(self, base_state: AgentState) -> None:
        """Verify the NO_ACTION path (LLM returns text, no tool calls)."""
        mock = MockLLM(response=AIMessage(content="No profitable action found. Current prices are unfavourable."))
        node = _build_market_intelligence_node(mock, [])
        result = await node(base_state)

        # Should have analysis but no execution_payload
        assert result.get("analysis_result") is not None
        assert "No profitable" in result["analysis_result"]["summary"]

        # Routing should go to portfolio router (no tools → router)
        route = _needs_tool_execution({**base_state, **result})
        assert route == "portfolio_router_node"

    @pytest.mark.asyncio
    async def test_error_handling(self, base_state: AgentState) -> None:
        """Verify error handling in nodes sets error state."""
        failing_mock = MagicMock()
        failing_mock.bind_tools.return_value = failing_mock
        failing_mock.ainvoke = AsyncMock(side_effect=RuntimeError("LLM failed"))

        node = _build_market_intelligence_node(failing_mock, [])
        result = await node(base_state)

        assert result.get("error") is not None
        assert "failed" in result["error"]

    def test_routing_has_tool_calls(self) -> None:
        """_needs_tool_execution routes to market_tools_node when tools called."""
        state: AgentState = {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[{"name": "search_cognitive_memory", "args": {"query": "test"}, "id": "1"}],
                )
            ],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        }

        route = _needs_tool_execution(state)
        assert route == "market_tools_node"

    def test_routing_no_tool_calls(self) -> None:
        """_needs_tool_execution routes to portfolio_router_node when no tools called."""
        state: AgentState = {
            "messages": [AIMessage(content="No action needed.")],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        }

        route = _needs_tool_execution(state)
        assert route == "portfolio_router_node"

    def test_recursion_guard(self) -> None:
        """_check_recursion_limit returns True when limit exceeded."""
        messages = []
        for _ in range(15):
            messages.append(
                AIMessage(
                    content="",
                    tool_calls=[{"name": "search", "args": {}, "id": "1"}],
                )
            )
        state: AgentState = {
            "messages": messages,
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        }

        assert _check_recursion_limit(state, max_calls=10)

    def test_recursion_guard_within_limit(self) -> None:
        """_check_recursion_limit returns False when within limit."""
        messages = []
        for _ in range(3):
            messages.append(
                AIMessage(
                    content="",
                    tool_calls=[{"name": "search", "args": {}, "id": "1"}],
                )
            )
        state: AgentState = {
            "messages": messages,
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        }

        assert not _check_recursion_limit(state, max_calls=10)

    def test_route_after_portfolio_tools(self) -> None:
        """_route_after_portfolio_tools always routes to END."""
        assert _route_after_portfolio_tools({}) == "done"

    def test_route_after_portfolio_no_tools(self) -> None:
        """_route_after_portfolio routes to done when no tool calls."""
        state: AgentState = {
            "messages": [AIMessage(content="No action needed.")],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "portfolio_routing",
        }
        route = _route_after_portfolio(state)
        assert route == "done"

    def test_route_after_portfolio_with_tools(self) -> None:
        """_route_after_portfolio routes to portfolio_tools_node when tools called."""
        state: AgentState = {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[{"name": "dispatch_execution", "args": {"target_contract": "0x0000000000000000000000000000000000000000"}, "id": "1"}],
                )
            ],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "portfolio_routing",
        }
        route = _route_after_portfolio(state)
        assert route == "portfolio_tools_node"
