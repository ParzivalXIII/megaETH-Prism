"""LangGraph state schemas for the MegaETH portfolio analysis agent.

Defines the ``AgentState`` TypedDict for graph state (messages, analysis
results, execution payloads) and ``AgentContext`` for runtime dependencies
(db, redis, qdrant, embedder, execution queue, subgraph client, llm).

These schemas are the contract between graph nodes — every node reads from
and writes to ``AgentState``, and receives context via ``config["configurable"]``.
"""

from __future__ import annotations

from typing import Any, TypedDict

from langchain_core.messages import BaseMessage
from typing_extensions import Annotated

from app.schemas.intent import ExecutionPayload

# ---------------------------------------------------------------------------
# AgentState — the graph state
# ---------------------------------------------------------------------------


class AgentState(TypedDict):
    """The state that flows through the LangGraph agent.

    Each invocation creates a fresh state.  Messages are accumulated via
    the ``operator.add`` reducer.  Other fields are overwritten on each
    node transition.
    """

    messages: Annotated[list[BaseMessage], "operator.add"]
    """The conversation history (accumulated across nodes)."""

    analysis_result: dict[str, Any] | None
    """Structured analysis output from the market intelligence phase."""

    execution_payload: ExecutionPayload | None
    """The generated execution plan (if the agent decided to act)."""

    user_address: str
    """The target user address for portfolio queries (0x-prefixed)."""

    error: str | None
    """Error message if a node encountered a failure."""

    current_phase: str
    """The current phase of the agent: market_analysis | portfolio_routing | dispatching | done"""


# ---------------------------------------------------------------------------
# AgentContext — runtime dependencies (injected via config)
# ---------------------------------------------------------------------------


class AgentContext(TypedDict):
    """Runtime dependencies injected into the graph via ``config["configurable"]``.

    Every tool node reads its required clients from this context rather
    than importing them directly, enabling testability and graceful
    degradation.
    """

    db_session_factory: Any
    """An ``async_sessionmaker[AsyncSession]`` for PostgreSQL queries."""

    redis_client: Any
    """An ``redis.asyncio.Redis`` instance for stream operations."""

    qdrant_client: Any
    """A ``QdrantManager`` instance for vector similarity search."""

    embedder: Any
    """A ``CohereEmbedder`` instance for text embeddings."""

    execution_queue: Any
    """An ``ExecutionQueue`` instance for dispatching execution plans."""

    subgraph_client: Any
    """A ``SubgraphClient`` instance for historical on-chain queries."""

    llm: Any
    """A ``langchain_openai.ChatOpenAI`` instance bound to the agent's tools."""
