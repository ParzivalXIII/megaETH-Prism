"""Unit tests for agent state schemas.

Covers:
- AgentState schema validation and defaults
- Messages accumulation (Annotated operator.add)
- Error propagation across state transitions
"""

from __future__ import annotations

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

from app.agents.state import AgentState


class TestAgentState:
    def test_schema_has_required_fields(self) -> None:
        """Verify AgentState has all required annotations."""
        annotations = AgentState.__annotations__
        assert "messages" in annotations
        assert "analysis_result" in annotations
        assert "execution_payload" in annotations
        assert "user_address" in annotations
        assert "error" in annotations
        assert "current_phase" in annotations

    def test_messages_accumulation(self) -> None:
        """Verify messages accumulate correctly (operator.add)."""
        state: AgentState = {
            "messages": [HumanMessage(content="hello")],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        }

        # Append more messages (simulating operator.add)
        state["messages"] = state["messages"] + [AIMessage(content="response")]
        state["messages"] = state["messages"] + [AIMessage(content="response2")]

        assert len(state["messages"]) == 3
        assert isinstance(state["messages"][0], BaseMessage)
        assert isinstance(state["messages"][1], BaseMessage)
        assert isinstance(state["messages"][2], BaseMessage)

    def test_error_propagation(self) -> None:
        """Verify error field can be set and propagated."""
        state: AgentState = {
            "messages": [],
            "analysis_result": None,
            "execution_payload": None,
            "user_address": "0xabc",
            "error": None,
            "current_phase": "market_analysis",
        }

        # Simulate error in a node
        state["error"] = "LLM invocation failed"
        state["current_phase"] = "done"

        assert state["error"] == "LLM invocation failed"
        assert state["current_phase"] == "done"
