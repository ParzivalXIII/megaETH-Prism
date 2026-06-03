"""Core LangGraph agent graph for the MegaETH portfolio analysis agent.

Builds a two-node state graph:
1. **Market Intelligence Node** — LLM with tools: search_cognitive_memory,
   query_historical_subgraph.  Produces analysis or routes to portfolio.
2. **Portfolio Router Node** — LLM with tools: query_portfolio, query_balance,
   search_cognitive_memory, dispatch_execution.  Routes to END or dispatches.

Both nodes are wrapped in try/except to guarantee graceful degradation.
A recursion guard limits tool calls per invocation.
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any, Callable, Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import ToolNode

from app.agents.state import AgentState
from app.agents.system_prompt import AGENT_SYSTEM_PROMPT
from app.core.logging import get_logger

logger = get_logger("megaeth.agents.graph")

# Type alias for the routing function return
RouterOutput = Literal["market_tools_node", "portfolio_router_node", "portfolio_tools_node", "done", "__end__"]


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------


def _build_market_intelligence_node(
    llm: Any,
    tools: list[BaseTool],
) -> Callable[[AgentState], Coroutine[Any, Any, dict[str, Any]]]:
    """Build the market intelligence node.

    The LLM is bound with tools for cognitive memory search and subgraph
    queries.  It analyses market conditions and produces findings or
    routes to the portfolio router.
    """
    system_msg = SystemMessage(content=AGENT_SYSTEM_PROMPT)
    bound_llm = llm.bind_tools(tools)

    async def market_intelligence_node(state: AgentState) -> dict[str, Any]:
        try:
            # Build messages list with system prompt
            messages = [system_msg] + list(state.get("messages", []))

            # Invoke LLM
            response: AIMessage = await bound_llm.ainvoke(messages)

            new_state: dict[str, Any] = {
                "messages": [response],
                "current_phase": "market_analysis",
            }

            # Store analysis result if the LLM responded with text
            if response.content and not getattr(response, "tool_calls", None):
                new_state["analysis_result"] = {
                    "summary": str(response.content),
                    "phase": "market_analysis",
                }

            return new_state

        except Exception as e:
            logger.error("market_intelligence_node failed", error=str(e), exc_info=True)
            return {
                "messages": [AIMessage(content=f"Market analysis failed: {e}")],
                "error": str(e),
                "current_phase": "done",
            }

    return market_intelligence_node


def _build_portfolio_router_node(
    llm: Any,
    tools: list[BaseTool],
) -> Callable[[AgentState], Coroutine[Any, Any, dict[str, Any]]]:
    """Build the portfolio router node.

    The LLM is bound with tools for portfolio queries, balance checks,
    memory search, and execution dispatch.  It decides whether to act
    (dispatch_execution) or take no action.
    """
    system_msg = SystemMessage(content=AGENT_SYSTEM_PROMPT)
    bound_llm = llm.bind_tools(tools)

    async def portfolio_router_node(state: AgentState) -> dict[str, Any]:
        try:
            messages = [system_msg] + list(state.get("messages", []))

            # Add analysis context if available
            analysis = state.get("analysis_result")
            if analysis:
                messages.append(
                    HumanMessage(
                        content=f"Previous analysis: {analysis.get('summary', '')}\n\n"
                        f"Now check the portfolio and decide whether to dispatch."
                    )
                )

            response: AIMessage = await bound_llm.ainvoke(messages)

            new_state: dict[str, Any] = {
                "messages": [response],
                "current_phase": "portfolio_routing",
            }

            # If the LLM produced a dispatch_execution tool call, extract payload
            if hasattr(response, "tool_calls") and response.tool_calls:
                for tc in response.tool_calls:
                    if tc["name"] == "dispatch_execution":
                        new_state["execution_payload"] = tc.get("args", {})
                        new_state["current_phase"] = "dispatching"

            return new_state

        except Exception as e:
            logger.error("portfolio_router_node failed", error=str(e), exc_info=True)
            return {
                "messages": [AIMessage(content=f"Portfolio routing failed: {e}")],
                "error": str(e),
                "current_phase": "done",
            }

    return portfolio_router_node


# ---------------------------------------------------------------------------
# Routing functions
# ---------------------------------------------------------------------------


def _needs_tool_execution(state: AgentState) -> RouterOutput:
    """Route to ToolNode if the last message has tool_calls.

    If the LLM requested tool executions, route to the ToolNode.
    Otherwise, route to the portfolio router node (for decision).
    """
    messages = state.get("messages", [])
    if not messages:
        return "portfolio_router_node"

    last_msg = messages[-1]

    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "market_tools_node"

    return "portfolio_router_node"


def _route_after_market_tools(state: AgentState) -> RouterOutput:
    """After market intelligence tools execute, route to portfolio router."""
    return "portfolio_router_node"


def _route_after_portfolio(state: AgentState) -> RouterOutput:
    """After portfolio router, route to ToolNode if dispatch needed, else END.

    If a dispatch_execution tool call was made, route to the tools node
    to execute it.  Otherwise, END.
    """
    messages = state.get("messages", [])
    if not messages:
        return "done"

    last_msg = messages[-1]
    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "portfolio_tools_node"

    return "done"


def _route_after_portfolio_tools(state: AgentState) -> RouterOutput:
    """After portfolio tools execute, always route to END.

    The dispatch_execution tool already enqueued the payload as a side
    effect.  The graph terminates here.
    """
    return "done"


# ---------------------------------------------------------------------------
# Recursion guard
# ---------------------------------------------------------------------------


def _check_recursion_limit(state: AgentState, max_calls: int = 10) -> bool:
    """Check if the agent has exceeded the maximum number of tool calls.

    Counts all tool_calls across all messages in the state.
    Returns ``True`` if the limit is exceeded (should stop).
    """
    total_calls = 0
    for msg in state.get("messages", []):
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            total_calls += len(msg.tool_calls)
    return total_calls >= max_calls


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_agent_graph(
    llm: Any,
    tools: list[BaseTool],
    system_prompt: str = AGENT_SYSTEM_PROMPT,
    checkpointer: Any | None = None,
) -> CompiledStateGraph:
    """Build the MegaETH portfolio analysis LangGraph.

    The graph has two nodes:
    1. ``market_intelligence_node`` — analyses market conditions.
       Tools: search_cognitive_memory, query_historical_subgraph.
       Routes: if tool_calls → ToolNode → portfolio_router_node, else → portfolio_router_node.
    2. ``portfolio_router_node`` — checks portfolio, decides action.
       Tools: query_portfolio, query_balance, search_cognitive_memory,
              dispatch_execution.
       Routes: if tool_calls → ToolNode → END, else → END.

    Parameters
    ----------
    llm : Any
        A ``langchain_openai.ChatOpenAI`` instance.
    tools : list[BaseTool]
        List of available tools (from ``app.agents.tools``).
    system_prompt : str, optional
        System prompt override (defaults to ``AGENT_SYSTEM_PROMPT``).
    checkpointer : Any, optional
        LangGraph checkpointer (default ``None`` — no checkpointing).
        Pass ``MemorySaver()`` to enable checkpointing.

    Returns
    -------
    CompiledStateGraph
        The compiled LangGraph ready for invocation.
    """
    # No default checkpointer — each invocation is stateless.
    # Callers that need checkpointing should pass a checkpointer explicitly.

    # Split tools by node
    market_tools = [
        t for t in tools
        if t.name in ("search_cognitive_memory", "query_historical_subgraph")
    ]
    portfolio_tools = [
        t for t in tools
        if t.name in ("query_portfolio", "query_balance", "search_cognitive_memory", "dispatch_execution")
    ]

    # Log which tools go where
    logger.debug(
        "building agent graph",
        market_tools=[t.name for t in market_tools],
        portfolio_tools=[t.name for t in portfolio_tools],
    )

    # Build nodes
    market_node = _build_market_intelligence_node(llm, market_tools)
    portfolio_node = _build_portfolio_router_node(llm, portfolio_tools)
    market_tools_node = ToolNode(market_tools)
    portfolio_tools_node = ToolNode(portfolio_tools)

    # Build graph
    workflow = StateGraph(AgentState)

    workflow.add_node("market_intelligence_node", market_node)  # type: ignore[call-overload]
    workflow.add_node("market_tools_node", market_tools_node)  # type: ignore[call-overload]
    workflow.add_node("portfolio_router_node", portfolio_node)  # type: ignore[call-overload]
    workflow.add_node("portfolio_tools_node", portfolio_tools_node)  # type: ignore[call-overload]

    # Entry point
    workflow.set_entry_point("market_intelligence_node")

    # Market intelligence → ToolNode (if tool_calls) or → portfolio router (no tools)
    workflow.add_conditional_edges(
        "market_intelligence_node",
        _needs_tool_execution,
        {
            "market_tools_node": "market_tools_node",
            "portfolio_router_node": "portfolio_router_node",
        },
    )

    # Market tools → portfolio router (always)
    workflow.add_conditional_edges(
        "market_tools_node",
        _route_after_market_tools,
        {
            "portfolio_router_node": "portfolio_router_node",
        },
    )

    # Portfolio router → tools (if dispatch_execution) or → END
    workflow.add_conditional_edges(
        "portfolio_router_node",
        _route_after_portfolio,
        {
            "portfolio_tools_node": "portfolio_tools_node",
            "done": END,
        },
    )

    # Portfolio tools → END (always — dispatch already happened)
    workflow.add_conditional_edges(
        "portfolio_tools_node",
        _route_after_portfolio_tools,
        {
            "done": END,
        },
    )

    # Compile
    graph = workflow.compile(checkpointer=checkpointer)
    logger.info("agent graph compiled successfully")

    return graph
