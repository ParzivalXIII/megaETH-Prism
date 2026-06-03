"""Agent API routes for manual / automated graph invocation.

All routes are **local-only** (no auth) — this API is intended for
development, testing, and automated tooling within the same Docker
network.  Do not expose this directly to the internet.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.api.dependencies import get_agent_graph

router = APIRouter(prefix="/agent", tags=["agent"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class AgentInvokeRequest(BaseModel):
    """Request body for ``POST /agent/invoke``.

    Attributes
    ----------
    user_address : str
        0x-prefixed user address to analyse.
    prompt : str, optional
        Additional prompt or instruction for the agent.
    """

    user_address: str = Field(
        ...,
        description="0x-prefixed user address to analyse",
        min_length=40,
        max_length=42,
    )
    prompt: str = Field(
        default="Analyse the portfolio and market conditions. "
        "Look for swap opportunities.",
        description="Natural language prompt for the agent",
    )


class AgentInvokeResponse(BaseModel):
    """Response from a single agent invocation.

    Attributes
    ----------
    status : str
        ``"success"`` or ``"error"``.
    execution_payload : dict | None
        The generated execution payload if the agent decided to act.
    analysis : str | None
        The agent's analysis summary.
    error : str | None
        Error message if something went wrong.
    current_phase : str
        The agent's final phase.
    """

    status: str = "success"
    execution_payload: dict[str, Any] | None = None
    analysis: str | None = None
    error: str | None = None
    current_phase: str = ""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/invoke", response_model=AgentInvokeResponse)
async def invoke_agent(
    request: AgentInvokeRequest,
    graph: Any = Depends(get_agent_graph),
) -> AgentInvokeResponse:
    """Invoke the LangGraph agent with a user address and optional prompt.

    This is a synchronous-style endpoint — it waits for the full graph
    execution to complete before returning.  For production use, the
    ``AgentRunner`` poll loop (in ``app/agents/runner.py``) is preferred.

    **Security**: No authentication — local network only.
    """
    if graph is None:
        return AgentInvokeResponse(
            status="error",
            error="Agent graph not initialised. Check OPENCODE_GO_API_KEY.",
        )

    from langchain_core.messages import HumanMessage

    try:
        # Build initial state
        messages = [HumanMessage(content=request.prompt)] if request.prompt else []
        initial_state = {
            "messages": messages,
            "analysis_result": None,
            "execution_payload": None,
            "user_address": request.user_address.lower(),
            "error": None,
            "current_phase": "market_analysis",
        }

        # Invoke graph
        result = await graph.ainvoke(
            initial_state,
            config={"configurable": {"user_address": request.user_address.lower()}},
        )

        # Extract response
        payload = result.get("execution_payload")
        error = result.get("error")
        phase = result.get("current_phase", "")

        # Extract analysis from last message
        analysis = None
        messages = result.get("messages", [])
        if messages:
            last_msg = messages[-1]
            if hasattr(last_msg, "content") and last_msg.content:
                analysis = str(last_msg.content)
            elif hasattr(last_msg, "content") and not last_msg.content:
                analysis = "(tool call — agent decided to dispatch)"

        return AgentInvokeResponse(
            status="error" if error else "success",
            execution_payload=payload if isinstance(payload, dict) else None,
            analysis=analysis,
            error=error,
            current_phase=phase or "done",
        )

    except Exception as e:
        from app.core.logging import get_logger

        get_logger("megaeth.api.agent").error("agent invocation failed", error=str(e), exc_info=True)
        return AgentInvokeResponse(
            status="error",
            error=str(e),
        )
