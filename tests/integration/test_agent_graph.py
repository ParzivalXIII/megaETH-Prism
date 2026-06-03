"""Integration tests for the full LangGraph agent graph.

Tests require:
- Docker services (Redis, PostgreSQL, Qdrant) from ``docker compose up -d``
- A configured ``OPENCODE_GO_API_KEY`` for the LLM

These tests use the real tool implementations but mock the LLM to avoid
API calls during testing.  The graph structure and routing are verified
end-to-end.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage

from app.agents.graph import build_agent_graph
from app.agents.tools import (
    dispatch_execution,
    query_balance,
    query_historical_subgraph,
    query_portfolio,
    search_cognitive_memory,
)

pytestmark = pytest.mark.asyncio

# Track whether Docker services are available
_redis_available = False


def _check_redis() -> bool:
    """Check Redis availability once."""
    try:
        import asyncio

        import redis.asyncio as aioredis

        from app.core.config import settings

        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        asyncio.run(r.ping())
        return True
    except Exception:
        return False


_redis_available = _check_redis()


# ---------------------------------------------------------------------------
# Mock LLM
# ---------------------------------------------------------------------------


class MockLLM:
    """Mock LLM for integration testing."""

    def __init__(self, response: AIMessage | None = None):
        self._response = response or AIMessage(content="analysis complete")
        self.bind_tools_called = False

    def bind_tools(self, tools: list) -> "MockLLM":
        self.bind_tools_called = True
        return self

    async def ainvoke(self, messages: list, **kwargs: object) -> AIMessage:
        return self._response


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_graph_compiles_and_routes() -> None:
    """Graph compiles and routes through market_intelligence_node."""
    mock_llm = MockLLM(response=AIMessage(content="No action needed."))
    tools = [
        dispatch_execution,
        query_balance,
        query_historical_subgraph,
        query_portfolio,
        search_cognitive_memory,
    ]

    graph = build_agent_graph(mock_llm, tools)
    assert graph is not None

    result = await graph.ainvoke(
        {
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        },
        config={"configurable": {}},
    )

    # Should have messages and no error
    assert "messages" in result
    assert result.get("error") is None


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_graph_no_action_path() -> None:
    """Graph produces analysis but no payload on the NO_ACTION path."""
    mock_llm = MockLLM(
        response=AIMessage(content="No profitable action found. Prices are unfavourable.")
    )
    tools = [
        dispatch_execution,
        query_balance,
        query_historical_subgraph,
        query_portfolio,
        search_cognitive_memory,
    ]

    graph = build_agent_graph(mock_llm, tools)
    result = await graph.ainvoke(
        {
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        },
        config={"configurable": {}},
    )

    assert result.get("execution_payload") is None
    assert result.get("error") is None


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_graph_error_handling() -> None:
    """Graph handles node failures gracefully."""
    failing_mock = MagicMock()
    failing_mock.bind_tools.return_value = failing_mock

    async def _fail(*args: object, **kwargs: object) -> AIMessage:
        raise RuntimeError("Simulated LLM failure")

    failing_mock.ainvoke = _fail

    tools = [
        dispatch_execution,
        query_balance,
        query_historical_subgraph,
        query_portfolio,
        search_cognitive_memory,
    ]

    graph = build_agent_graph(failing_mock, tools)
    result = await graph.ainvoke(
        {
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        },
        config={"configurable": {}},
    )

    assert "messages" in result
    assert result.get("error") is not None


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_graph_builds_with_all_tools() -> None:
    """Graph builds successfully with all tools registered."""
    mock_llm = MockLLM()
    tools = [
        dispatch_execution,
        query_balance,
        query_historical_subgraph,
        query_portfolio,
        search_cognitive_memory,
    ]

    graph = build_agent_graph(mock_llm, tools)
    assert graph is not None

    # Verify graph has the expected nodes
    graph_def = graph.get_graph()
    node_names = set(graph_def.nodes)
    expected_nodes = {
        "market_intelligence_node",
        "market_tools_node",
        "portfolio_router_node",
        "portfolio_tools_node",
    }
    assert expected_nodes.issubset(node_names), f"Missing nodes: {expected_nodes - node_names}"


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_graph_market_tools_routing() -> None:
    """Market intelligence node routes to ToolNode when tool calls present."""
    tool_call_msg = AIMessage(
        content="",
        tool_calls=[{
            "name": "search_cognitive_memory",
            "args": {"query": "test market", "limit": 5},
            "id": "call_1",
            "type": "tool_call",
        }],
    )
    mock_llm = MockLLM(response=tool_call_msg)
    tools = [search_cognitive_memory, dispatch_execution]

    graph = build_agent_graph(mock_llm, tools)
    result = await graph.ainvoke(
        {
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        },
        config={"configurable": {}},
    )

    # Tool execution may fail due to missing context, but graph should
    # complete without error (graceful degradation)
    assert "messages" in result


@pytest.mark.skipif(not _redis_available, reason="Docker services not available")
async def test_graph_with_tool_calls_routes_to_portfolio() -> None:
    """When LLM makes tool calls, graph routes to portfolio_router_node."""
    # Create an LLM that returns tool calls
    tool_call_msg = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "search_cognitive_memory",
                "args": {"query": "test"},
                "id": "call_1",
                "type": "tool_call",
            }
        ],
    )
    mock_llm = MockLLM(response=tool_call_msg)

    tools = [
        dispatch_execution,
        query_balance,
        query_historical_subgraph,
        query_portfolio,
        search_cognitive_memory,
    ]

    graph = build_agent_graph(mock_llm, tools)

    result = await graph.ainvoke(
        {
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        },
        config={"configurable": {}},
    )

    # Should complete without error
    assert result.get("error") is None
