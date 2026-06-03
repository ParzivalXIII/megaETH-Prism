"""Unit tests for agent tool callables.

Covers:
- Tool decorator validation (tools have names, are async)
- Dict shape returns (tools return dicts with expected keys)
- Error handling (tools return error dicts on missing context)
"""

from __future__ import annotations

import pytest

from app.agents.tools import (
    dispatch_execution,
    query_balance,
    query_historical_subgraph,
    query_portfolio,
    search_cognitive_memory,
)


class TestToolDecorators:
    def test_all_tools_have_names(self) -> None:
        """Verify all tools have non-empty names."""
        tools = [
            dispatch_execution,
            query_balance,
            query_historical_subgraph,
            query_portfolio,
            search_cognitive_memory,
        ]
        for tool in tools:
            assert tool.name, f"Tool {tool} has no name"
            assert len(tool.name) > 0, f"Tool {tool} has empty name"

    @pytest.mark.asyncio
    async def test_all_tools_are_callable(self) -> None:
        """Verify all tools can be invoked (async ainvoke)."""
        tools = [
            dispatch_execution,
            query_balance,
            query_historical_subgraph,
            query_portfolio,
            search_cognitive_memory,
        ]
        for tool in tools:
            # StructuredTool with coroutine should be usable with ainvoke
            assert tool.coroutine is not None, f"Tool {tool.name} has no coroutine"

    @pytest.mark.asyncio
    async def test_query_portfolio_returns_error_on_missing_context(self) -> None:
        """Verify query_portfolio returns error dict without context."""
        result = await query_portfolio.ainvoke(
            {"user_address": "0xabc", "token_symbol": "", "config": None}
        )
        assert isinstance(result, dict)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_query_balance_returns_error_on_missing_context(self) -> None:
        """Verify query_balance returns error dict without context."""
        result = await query_balance.ainvoke(
            {"user_address": "0xabc", "token_address": "0xdef", "config": None}
        )
        assert isinstance(result, dict)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_search_memory_returns_error_on_missing_context(self) -> None:
        """Verify search_cognitive_memory returns error dict without context."""
        result = await search_cognitive_memory.ainvoke(
            {"query": "test query", "limit": 5, "config": None}
        )
        assert isinstance(result, dict)
        assert "error" in result

    @pytest.mark.asyncio
    async def test_dispatch_returns_error_on_missing_context(self) -> None:
        """Verify dispatch_execution returns error dict without context."""
        result = await dispatch_execution.ainvoke(
            {"payload": {}, "config": None}
        )
        assert isinstance(result, dict)
        assert "error" in result
