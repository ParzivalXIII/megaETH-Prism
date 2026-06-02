# Phase 2 Architecture Research: LangGraph Agents on MegaETH

## Summary of Findings

The existing Phase 1 infrastructure (Redis Streams buffer, PostgreSQL state tracker, Qdrant vector indexer)
provides all the building blocks needed for Phase 2 agent infrastructure. The patterns below show how to
layer LangGraph agents on top, respecting the Architecture: Strategy/Reflex Split from AGENTS.md —
agents do cognition (500-3000ms), workers do execution (<1ms).

---

## 1. LangGraph State Machine Patterns

### 1.1 StateGraph with TypedDict State

The canonical LangGraph pattern uses `StateGraph` with a `TypedDict` state schema. Each node receives
the current state and returns partial updates. Reducers (via `Annotated`) control how updates merge.

```python
"""
app/agents/state.py — Shared agent state schemas for LangGraph Phase 2.

LangGraph 1.0+ uses TypedDict-based state with Annotated reducers for each field.
StateGraph(state_schema=State, context_schema=Context) pattern is current as of langgraph>=1.2.2.
"""
from __future__ import annotations

from operator import add
from typing import Annotated, Any, Sequence

from langchain_core.messages import BaseMessage
from typing_extensions import TypedDict


class AgentState(TypedDict):
    """Core agent state — messages accumulate via operator.add reducer."""
    messages: Annotated[Sequence[BaseMessage], add]
    intent: str | None
    execution_plan: dict[str, Any] | None
    last_tool_output: dict[str, Any] | None


class AgentContext(TypedDict):
    """Immutable runtime context injected at invoke time — never modified by nodes."""
    user_id: str
    thread_id: str
    db_session_factory: Any  # async_sessionmaker
    redis_client: Any         # redis.asyncio.Redis
    qdrant_client: Any        # QdrantManager (sync client or async wrapper)


class IntentRoutingState(TypedDict):
    """Specialised state for intent-routing sub-graph."""
    raw_query: str
    resolved_intents: list[str]
    tool_calls: list[dict[str, Any]]
```

### 1.2 Intent Router — Decomposing Commands into Multi-Hop Topologies

The intent router node uses an LLM with `with_structured_output()` to classify user intents into a
known taxonomy, then decomposes composite intents into ordered steps via conditional edges.

```python
"""
app/agents/routers/intent_router.py — Classifies user intents and routes to specialist agents.

The router node:
1. Calls an LLM with structured output to classify intents
2. Returns a routing key used by conditional edges
3. For composite intents, decomposes into ordered sub-goals

Key LangGraph patterns:
- with_structured_output(Pydantic model) — forces LLM output into a typed schema
- add_conditional_edges with path_map — routes to different agent nodes based on intent
"""
from __future__ import annotations

from langchain_core.output_parsers import BaseOutputParser
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, Field


class IntentClassification(BaseModel):
    """Structured output from the intent classification LLM call."""
    primary_intent: str = Field(description="Primary intent: portfolio_query|trade|market_analysis|memory_retrieval|unknown")
    confidence: float = Field(ge=0.0, le=1.0)
    sub_intents: list[str] = Field(default_factory=list, description="Ordered sub-goals for multi-hop queries")
    entities: dict[str, str] = Field(default_factory=dict, description="Extracted entities: token, amount, timeframe")


class IntentRouterAgent:
    """LangGraph agent that classifies user intents and decomposes multi-hop commands.

    The router is designed to NEVER call web3.py directly — it writes execution plans
    to Redis for downstream async worker pools to pick up.
    """

    def __init__(self, llm, redis_client, db_session_factory):
        self.llm = llm.with_structured_output(IntentClassification)
        self.redis = redis_client
        self.db_session_factory = db_session_factory

    async def classify_intent(self, state: IntentRoutingState) -> dict:
        """Node: classify the user's raw query using structured LLM output."""
        result: IntentClassification = await self.llm.ainvoke(
            f"Classify this user command: {state['raw_query']}"
        )
        return {
            "resolved_intents": [result.primary_intent] + result.sub_intents,
        }

    async def decompose_to_plan(self, state: IntentRoutingState) -> dict:
        """Node: decompose classified intents into an execution plan for workers."""
        plan = {
            "intents": state["resolved_intents"],
            "steps": [],
        }
        for intent in state["resolved_intents"]:
            if intent == "portfolio_query":
                plan["steps"].append({
                    "action": "query_portfolio",
                    "tool": "postgres",
                    "table": "asset_balances",
                })
            elif intent == "market_analysis":
                plan["steps"].append({
                    "action": "query_historical",
                    "tool": "graphql_subgraph",
                })
            elif intent == "memory_retrieval":
                plan["steps"].append({
                    "action": "search_memory",
                    "tool": "qdrant_vector",
                })
        return {"execution_plan": plan}

    async def route_by_intent(self, state: IntentRoutingState) -> str:
        """Conditional edge function: returns the next node based on primary intent."""
        if not state.get("resolved_intents"):
            return "llm_fallback"
        primary = state["resolved_intents"][0]
        routing_map = {
            "portfolio_query": "portfolio_agent",
            "trade": "trade_analysis_agent",
            "market_analysis": "market_agent",
            "memory_retrieval": "memory_agent",
        }
        return routing_map.get(primary, "llm_fallback")

    def build_graph(self) -> StateGraph:
        """Build the intent routing sub-graph."""
        builder = StateGraph(IntentRoutingState)
        builder.add_node("classify_intent", self.classify_intent)
        builder.add_node("decompose_plan", self.decompose_to_plan)

        # Edge routing: classify -> decompose -> conditional route
        builder.add_edge(START, "classify_intent")
        builder.add_edge("classify_intent", "decompose_plan")
        builder.add_conditional_edges(
            "decompose_plan",
            self.route_by_intent,
            {
                "portfolio_agent": "portfolio_agent",
                "trade_analysis_agent": "trade_analysis_agent",
                "market_agent": "market_agent",
                "memory_agent": "memory_agent",
                "llm_fallback": "llm_fallback",
            },
        )
        return builder
```

### 1.3 Tool-Calling Agent with Structured JSON Output

LangGraph agents use `@tool` decorators or plain async functions. The `create_react_agent` prebuilt
handles the agent loop (LLM call -> tool call -> LLM call). For custom graphs, use `ToolNode`.

```python
"""
app/agents/tools/local_query_tool.py — Tools that the Portfolio & Market Analysis Agent can call.

All tools MUST:
- Return structured data (dict or Pydantic model) — never call web3.py
- Be annotated with clear descriptions so the LLM knows when to invoke them
- Accept a config/context parameter for dependency injection
"""
from __future__ import annotations

from typing import Any

from langchain_core.tools import tool
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.state.database import create_async_engine_pool


@tool
async def query_local_state(
    user_id: str,
    token_symbol: str = "",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Query PostgreSQL for current asset balances and positions for a user.

    Args:
        user_id: The user's address or identifier.
        token_symbol: Optional token symbol filter (e.g., "USDM", "WETH").
        config: Runtime config with db_session_factory.
    """
    session_factory = config.get("db_session_factory") if config else None
    if session_factory is None:
        from app.state.database import create_async_engine_pool
        _, session_factory = create_async_engine_pool()

    async with session_factory() as session:
        stmt = select(AssetBalance).where(AssetBalance.user_id == user_id)
        if token_symbol:
            stmt = stmt.where(AssetBalance.token_symbol == token_symbol)

        result = await session.execute(stmt)
        balances = result.scalars().all()

    return {
        "user_id": user_id,
        "balances": [
            {"token": b.token_symbol, "amount": str(b.amount), "last_updated": b.updated_at.isoformat()}
            for b in balances
        ],
        "count": len(balances),
    }


@tool
async def search_cognitive_memory(
    query: str,
    user_id: str = "",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Search Qdrant vector database for historical context and past agent decisions.

    Args:
        query: Natural language search query for semantic similarity.
        user_id: Optional user filter for memory scoping.
        config: Runtime config with embedder and qdrant_client.
    """
    embedder = config.get("embedder") if config else None
    qdrant = config.get("qdrant_client") if config else None

    if embedder is None or qdrant is None:
        raise ValueError("embedder and qdrant_client must be provided via config")

    embedding = await embedder.embed(query)

    filters = None
    if user_id:
        from qdrant_client.models import Filter, FieldCondition, MatchValue
        filters = Filter(must=[FieldCondition(key="user_id", match=MatchValue(value=user_id))])

    results = qdrant.search(vector=embedding, limit=5, query_filter=filters)
    return {
        "query": query,
        "results": [
            {"id": r.id, "score": r.score, "summary": r.payload.get("summary", "")}
            for r in results
        ],
        "count": len(results),
    }


@tool
async def query_historical_subgraph(
    token_address: str,
    timeframe: str = "7d",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Query The Graph or similar subgraph for historical token data.

    Args:
        token_address: Contract address of the token to query.
        timeframe: Lookback period (1d, 7d, 30d, 90d).
        config: Runtime config with subgraph URL.
    """
    from gql import Client, gql
    from gql.transport.aiohttp import AIOHTTPTransport

    subgraph_url = config.get("subgraph_url", "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3") if config else None
    transport = AIOHTTPTransport(url=subgraph_url)

    async with Client(transport=transport) as session:
        query = gql("""
            query TokenDayData($token: String!, $days: Int!) {
                tokenDayDatas(
                    first: $days,
                    orderBy: date,
                    orderDirection: desc,
                    where: { token: $token }
                ) {
                    date
                    priceUSD
                    volumeUSD
                    totalValueLockedUSD
                }
            }
        """)
        days = {"1d": 1, "7d": 7, "30d": 30, "90d": 90}.get(timeframe, 7)
        result = await session.execute(
            query, variable_values={"token": token_address.lower(), "days": days}
        )
    return {"token": token_address, "timeframe": timeframe, "data": result}


# ---------------------------------------------------------------------------
# SQLModel schema for the asset_balances table referenced by query_local_state
# ---------------------------------------------------------------------------
from datetime import datetime
from sqlmodel import Field, SQLModel


class AssetBalance(SQLModel, table=True):
    __tablename__: str = "asset_balances"  # type: ignore[misc]

    user_id: str = Field(primary_key=True)
    token_address: str = Field(primary_key=True)
    token_symbol: str = Field(default="")
    amount: float = Field(default=0.0)
    decimals: int = Field(default=18)
    last_block_number: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
```

### 1.4 Building the Full Agent Graph — Multi-Agent Orchestration

The complete agent graph combines the intent router with specialist sub-agents. The supervisor
pattern (`langgraph_supervisor.create_supervisor`) delegates to specialist agents, each with its
own tool set.

```python
"""
app/agents/orchestrator.py — Top-level LangGraph agent graph.

Architecture:
    User Input -> Intent Router (classify + decompose)
                      |
           +----------+-----------+
           |          |           |
      Portfolio   Market    Memory
       Agent      Agent     Agent
           |          |           |
           +----------+-----------+
                      |
              Execution Plan -> Redis Queue (NOT web3.py)

All agents follow the Strategy/Reflex Split:
- Cognition (LangGraph): 500-3000ms — macro analysis, planning, intent resolution
- Execution (Worker Pool): <1ms — deterministic target eval, tx assembly
"""
from __future__ import annotations

from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver

from app.agents.state import AgentState, AgentContext


def build_agent_graph(
    llm,
    tools: list,
    system_prompt: str,
    checkpointer=None,
) -> StateGraph:
    """Build a complete agent graph with tool-calling and checkpointing.

    Args:
        llm: Chat model (e.g., ChatOpenAI, ChatAnthropic).
        tools: List of @tool-decorated async functions.
        system_prompt: System prompt injected at the start.
        checkpointer: Optional persistent checkpointer (MemorySaver for dev).

    Returns:
        Compiled StateGraph ready for .ainvoke().

    Graph topology:
        START -> preprocess -> agent -> tools_condition -> tools -> agent
                                                    |-> END (no tool calls)
    """
    if checkpointer is None:
        checkpointer = MemorySaver()

    llm_with_tools = llm.bind_tools(tools)

    async def preprocess_node(state: AgentState) -> dict:
        """Inject system prompt and any pre-run analysis."""
        messages = list(state.get("messages", []))
        if messages and isinstance(messages[0], SystemMessage):
            return {}  # Already has system prompt
        return {"messages": [SystemMessage(content=system_prompt)] + messages}

    def agent_node(state: AgentState) -> dict:
        """LLM node — generates next message (may include tool_calls)."""
        response = llm_with_tools.invoke(state["messages"])
        return {"messages": [response]}

    tool_node = ToolNode(tools)

    builder = StateGraph(AgentState)
    builder.add_node("preprocess", preprocess_node)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tool_node)

    builder.add_edge(START, "preprocess")
    builder.add_edge("preprocess", "agent")
    builder.add_conditional_edges(
        "agent",
        tools_condition,  # built-in: routes to "tools" if there are tool_calls, else END
        {"tools": "tools", END: END},
    )
    builder.add_edge("tools", "agent")

    return builder.compile(checkpointer=checkpointer)


# ---------------------------------------------------------------------------
# FastAPI integration: invoke graph in a route/background task
# ---------------------------------------------------------------------------
async def invoke_agent(
    graph,
    user_message: str,
    thread_id: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Invoke the compiled graph asynchronously with thread-based checkpointing."""
    config = {"configurable": {"thread_id": thread_id}}
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content=user_message)]},
        config=config,
        context=context,
    )
    return result
```

---

## 2. LangGraph + FastAPI Integration

### 2.1 Direct Graph Invocation in FastAPI Routes

The compiled graph implements the `Runnable` interface. In a FastAPI route, call `.ainvoke()` directly.
The routing layer should be thin — delegate all business logic to the agent graph.

```python
"""
app/api/routes/agent.py — FastAPI route that invokes LangGraph agents.

This route is the entry point for user commands. It:
1. Validates input via Pydantic
2. Invokes the compiled agent graph asynchronously
3. Returns the agent's structured response
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.api.dependencies import get_db_session_factory, get_redis_client, get_qdrant_client, get_embedder, get_agent_graph
from app.agents.state import AgentContext

router = APIRouter(prefix="/agent", tags=["agent"])


@router.post("/command")
async def agent_command(
    command: str,
    thread_id: str = "default",
    graph=Depends(get_agent_graph),
    db_session_factory=Depends(get_db_session_factory),
    redis_client=Depends(get_redis_client),
    qdrant_client=Depends(get_qdrant_client),
    embedder=Depends(get_embedder),
) -> dict[str, Any]:
    """Submit a natural-language command to the agent system.

    The agent will:
    - Classify intent
    - Query local state (PostgreSQL), historical data (GraphQL subgraph),
      and cognitive memory (Qdrant) via tool calls
    - Generate an execution plan written to Redis for worker pools
    """
    context = AgentContext(
        user_id="default_user",  # TODO: extract from auth
        thread_id=thread_id,
        db_session_factory=db_session_factory,
        redis_client=redis_client,
        qdrant_client=qdrant_client,
    )

    config = {"configurable": {"thread_id": thread_id}}
    try:
        result = await graph.ainvoke(
            {"messages": [{"role": "user", "content": command}]},
            config=config,
            context=context,
        )
        return {"status": "ok", "thread_id": thread_id, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/thread/{thread_id}")
async def get_thread_state(
    thread_id: str,
    graph=Depends(get_agent_graph),
) -> dict[str, Any]:
    """Get the current state of an agent thread (for resumable sessions)."""
    config = {"configurable": {"thread_id": thread_id}}
    state = await graph.aget_state(config)
    return {"thread_id": thread_id, "state": state.values if state else None}
```

### 2.2 Background Agent Runs (Fire-and-Forget)

For long-running agent tasks (e.g., portfolio rebalancing analysis), use FastAPI's `BackgroundTasks`
or manually create asyncio tasks that write results to a known Redis key.

```python
"""
app/api/routes/background_agent.py — Background (non-blocking) agent invocations.

Pattern: POST triggers agent run in background; result is polled via GET.
This avoids blocking the HTTP connection for 2-3 seconds of LLM inference.
"""
from __future__ import annotations

import asyncio
import json
import uuid

from fastapi import APIRouter, BackgroundTasks, HTTPException

router = APIRouter(prefix="/agent", tags=["agent"])


@router.post("/command/async")
async def agent_command_async(
    command: str,
    background_tasks: BackgroundTasks,
    graph=...,
    redis_client=...,
) -> dict:
    """Submit a command to be processed in background. Returns a task_id."""
    task_id = str(uuid.uuid4())

    # Pre-allocated result placeholder
    await redis_client.set(f"agent:task:{task_id}", json.dumps({"status": "pending"}), ex=3600)

    async def _run_agent():
        try:
            config = {"configurable": {"thread_id": task_id}}
            result = await graph.ainvoke(
                {"messages": [{"role": "user", "content": command}]},
                config=config,
            )
            await redis_client.set(
                f"agent:task:{task_id}",
                json.dumps({"status": "completed", "result": result}, default=str),
                ex=3600,
            )
        except Exception as e:
            await redis_client.set(
                f"agent:task:{task_id}",
                json.dumps({"status": "failed", "error": str(e)}),
                ex=3600,
            )

    background_tasks.add_task(_run_agent)
    return {"task_id": task_id, "status": "accepted"}


@router.get("/task/{task_id}")
async def get_task_result(task_id: str, redis_client=...) -> dict:
    """Poll for the result of a background agent task."""
    raw = await redis_client.get(f"agent:task:{task_id}")
    if raw is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return json.loads(raw)
```

### 2.3 Dependency Injection Setup

```python
"""
app/api/dependencies.py — Shared FastAPI dependencies for agent routes.

Phase 2 will populate this with singletons: compiled graph, db session factory,
redis client, qdrant manager, embedder.
"""
from __future__ import annotations

from functools import lru_cache

from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.agents.orchestrator import build_agent_graph
from app.agents.tools.local_query_tool import query_local_state, search_cognitive_memory, query_historical_subgraph
from app.core.config import settings
from app.state.database import create_async_engine_pool
from app.streams.buffer import StreamBuffer
from app.vector.embedder import CohereEmbedder
from app.vector.qdrant_client import QdrantManager


# Cached singletons (created once per process)
_graph = None
_embedder = None
_qdrant = None


async def get_db_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the async session factory (cached)."""
    _, factory = create_async_engine_pool()
    return factory


async def get_redis_client():
    """Return a shared Redis client for writing to execution queues."""
    import redis.asyncio as redis
    return redis.from_url(settings.redis_url, decode_responses=True)


async def get_qdrant_client():
    """Return shared QdrantManager instance."""
    global _qdrant
    if _qdrant is None:
        _qdrant = QdrantManager()
        _qdrant.initialize()
    return _qdrant


async def get_embedder():
    """Return shared CohereEmbedder instance."""
    global _embedder
    if _embedder is None:
        _embedder = CohereEmbedder()
    return _embedder


async def get_agent_graph():
    """Return the compiled LangGraph agent graph (singleton)."""
    global _graph
    if _graph is None:
        from langchain_openai import ChatOpenAI
        llm = ChatOpenAI(model="gpt-4o", temperature=0)
        tools = [query_local_state, search_cognitive_memory, query_historical_subgraph]
        _graph = build_agent_graph(
            llm=llm,
            tools=tools,
            system_prompt=(
                "You are a portfolio analysis agent on MegaETH. "
                "You have access to PostgreSQL (asset balances), "
                "Qdrant (historical memory), and The Graph (market data). "
                "Never sign transactions directly — always write execution "
                "plans to Redis for the async worker pool to execute."
            ),
        )
    return _graph
```

---

## 3. Redis as Execution Queue from LangGraph

### 3.1 Pattern: LangGraph writes JSON to Redis, Worker Pool Reads

This is the **Strategy/Reflex Split** boundary. Agent nodes in the graph write structured JSON
execution plans to Redis lists or streams. The async worker pool (running in separate asyncio tasks)
pops and executes them. This keeps the LLM-based agent out of the hot path.

```python
"""
app/agents/dispatch.py — Execution plan dispatch from LangGraph to Redis.

Agents NEVER call web3.py directly. They produce JSON payloads written to Redis
lists or streams. A separate async worker pool pops and executes them.

Redis key design:
- megaeth:queue:executions   — Redis LIST (LPUSH/RPOP) for FIFO execution
- megaeth:stream:executions  — Redis STREAM for persistent ordered execution
- megaeth:executions:{id}    — Redis HASH for execution status/results
"""
from __future__ import annotations

import json
import uuid
from typing import Any

import redis.asyncio as redis
from pydantic import BaseModel, Field


class ExecutionPayload(BaseModel):
    """Deterministic execution definition written by agents, read by workers.

    Agents produce this JSON. Workers consume it, validate conditions,
    assemble and broadcast transactions.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    target_contract: str
    call_data: str
    max_gas_price: int = 1_000_000  # MegaETH: 0.001 gwei base
    trigger_condition_schema: dict[str, Any] = Field(default_factory=dict)
    max_slippage_bps: int = 100  # 1% hard cap per AGENTS.md
    priority: str = "normal"  # "normal" | "high" | "low"
    created_by_agent: str = ""
    created_at: float = Field(default_factory=__import__("time").time)


class ExecutionQueue:
    """Redis-backed execution queue for LangGraph -> Worker communication.

    Agents call ``enqueue()`` to write an execution plan. Workers call
    ``dequeue()`` to pop the next job. Results are written via ``set_result()``.
    """

    QUEUE_KEY = "megaeth:queue:executions"
    RESULT_PREFIX = "megaeth:executions:"

    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client

    async def enqueue(self, payload: ExecutionPayload) -> str:
        """Agent writes an execution plan to the queue. Returns the execution ID."""
        data = payload.model_dump_json()
        await self._redis.lpush(self.QUEUE_KEY, data)
        # Initialize result placeholder
        await self._redis.hset(
            f"{self.RESULT_PREFIX}{payload.id}",
            mapping={"status": "pending", "created_at": str(payload.created_at)},
        )
        await self._redis.expire(f"{self.RESULT_PREFIX}{payload.id}", 86400)  # 24h TTL
        return payload.id

    async def dequeue(self, timeout: float = 5.0) -> ExecutionPayload | None:
        """Worker pops the next execution plan (blocking pop with timeout)."""
        result = await self._redis.brpop([self.QUEUE_KEY], timeout=timeout)
        if result is None:
            return None
        _, data = result
        return ExecutionPayload.model_validate_json(data)

    async def set_result(self, execution_id: str, status: str, tx_hash: str = "", error: str = ""):
        """Worker writes result after executing (or failing) the plan."""
        mapping = {"status": status}
        if tx_hash:
            mapping["tx_hash"] = tx_hash
        if error:
            mapping["error"] = error
        await self._redis.hset(f"{self.RESULT_PREFIX}{execution_id}", mapping=mapping)

    async def get_result(self, execution_id: str) -> dict[str, str] | None:
        """Agent or API route queries the result of an execution."""
        result = await self._redis.hgetall(f"{self.RESULT_PREFIX}{execution_id}")
        return result if result else None

    # ------------------------------------------------------------------
    # Alternative: Redis Streams pattern (for persistent, replayable queue)
    # ------------------------------------------------------------------
    EXECUTION_STREAM = "megaeth:stream:executions"
    EXECUTION_GROUP = "megaeth:workers:executors"

    async def enqueue_stream(self, payload: ExecutionPayload) -> str:
        """Alternative: enqueue using Redis Streams (persistent, consumer-group based)."""
        entry_id = await self._redis.xadd(
            self.EXECUTION_STREAM,
            {"payload": payload.model_dump_json()},
            maxlen=100_000,
        )
        return entry_id
```

### 3.2 Usage from a LangGraph Node

```python
"""
Example agent node that writes an execution plan to Redis.

This is the canonical pattern — the agent determines what to do, writes a plan,
and returns to the user. A worker pool executes the plan asynchronously.
"""
async def trade_execution_node(state: AgentState, *, config: RunnableConfig) -> dict:
    """Agent node: evaluate market conditions, produce execution plan, write to Redis."""
    # Get injected dependencies from runtime context / config
    redis_client = config.get("configurable", {}).get("redis_client")  # pragma: no cover
    if redis_client is None:
        raise ValueError("redis_client not in config")

    queue = ExecutionQueue(redis_client)

    # Agent logic: determine what to execute (this is where LLM reasoning happens)
    # In production, the preceding agent node would have set these in state
    execution = ExecutionPayload(
        target_contract="0x402085c248EeA27D92E8b30b2C58ed07f9E20001",  # x402ExactPermit2Proxy
        call_data="0x...",
        max_gas_price=1_000_000,
        trigger_condition_schema={
            "type": "price_threshold",
            "token": "0x...",
            "threshold": "1.50",
            "direction": "above",
        },
        max_slippage_bps=100,
        created_by_agent="portfolio_agent",
    )

    execution_id = await queue.enqueue(execution)

    return {
        "messages": [
            {"role": "assistant", "content": f"Trade execution plan {execution_id} queued for worker pool."}
        ],
        "last_tool_output": {"execution_id": execution_id, "status": "queued"},
    }
```

---

## 4. PostgreSQL Query Patterns for Real-Time Asset Balances

### 4.1 SQLModel Schema for Asset Balances

Design for the Phase 2 data model: per-user asset balances updated by block ingestion workers.

```python
"""
app/schemas/balances.py — SQLModel schemas for user asset balances.

Each row is a (user_id, token_address) pair with current balance.
Updated idempotently by the state-tracking worker on each relevant block.

Key design decisions:
- Composite PK (user_id, token_address) for efficient lookup
- ON CONFLICT DO UPDATE for idempotent upsert
- last_block_number tracks which block last updated this row
- Index on block_number for efficient bulk updates
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, func
from sqlmodel import Field, SQLModel

from app.schemas.mini_block import MiniBlockPayload


class AssetBalance(SQLModel, table=True):
    """Per-user asset balance tracked from on-chain events.

    Idempotent upsert via composite PK. Updated by the state tracker worker
    whenever a relevant Transfer/Approval event appears in a mini-block.
    """
    __tablename__: str = "asset_balances"  # type: ignore[misc]
    __table_args__ = (
        # Partial index for fast portfolio queries by user
        {"extend_existing": True},
    )

    user_address: str = Field(primary_key=True, max_length=42, description="0x-prefixed address")
    token_address: str = Field(primary_key=True, max_length=42, description="ERC-20 contract address")
    token_symbol: str = Field(default="", max_length=20, index=True)
    balance_raw: int = Field(default=0, description="Raw balance (no decimals applied)")
    decimals: int = Field(default=18)
    block_number: int = Field(default=0, description="Last block that updated this row")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), server_default=func.now()),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    )

    @property
    def balance_human(self) -> float:
        """Human-readable balance (raw / 10^decimals)."""
        return self.balance_raw / (10 ** self.decimals)


class TokenTransfer(SQLModel, table=True):
    """Raw token transfer events extracted from mini-block receipts.

    This is the raw event log — denormalized for query speed.
    AssetBalance is the materialized view / upsert target.
    """
    __tablename__: str = "token_transfers"  # type: ignore[misc]

    id: int | None = Field(default=None, primary_key=True)
    block_number: int = Field(index=True)
    tx_index: int = Field(default=0)
    log_index: int = Field(default=0)
    token_address: str = Field(max_length=42, index=True)
    from_address: str = Field(max_length=42, index=True)
    to_address: str = Field(max_length=42, index=True)
    amount: int
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), server_default=func.now()),
    )
```

### 4.2 Repository Pattern for Asset Balances

```python
"""
app/state/balance_repository.py — Upsert and query operations for asset balances.

Follows the existing StateRepository pattern from Phase 1.
"""
from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.balances import AssetBalance


class AssetBalanceRepository:
    """Repository for asset balance upserts and queries."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, balance: AssetBalance) -> None:
        """Idempotent upsert using PostgreSQL ON CONFLICT DO UPDATE.

        Uses native PostgreSQL INSERT ... ON CONFLICT for atomic upsert
        (more efficient than ORM merge() for bulk operations).
        """
        stmt = pg_insert(AssetBalance).values({
            "user_address": balance.user_address,
            "token_address": balance.token_address,
            "token_symbol": balance.token_symbol,
            "balance_raw": balance.balance_raw,
            "decimals": balance.decimals,
            "block_number": balance.block_number,
        }).on_conflict_do_update(
            index_elements=["user_address", "token_address"],
            set_={
                "balance_raw": balance.balance_raw,
                "token_symbol": balance.token_symbol,
                "block_number": balance.block_number,
                "updated_at": balance.updated_at,
            },
        )
        await self._session.execute(stmt)
        await self._session.commit()

    async def get_portfolio(self, user_address: str) -> list[AssetBalance]:
        """Get all asset balances for a user."""
        stmt = select(AssetBalance).where(AssetBalance.user_address == user_address)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_balance(self, user_address: str, token_address: str) -> AssetBalance | None:
        """Get a single asset balance."""
        stmt = select(AssetBalance).where(
            AssetBalance.user_address == user_address,
            AssetBalance.token_address == token_address,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def bulk_upsert(self, balances: list[AssetBalance]) -> int:
        """Efficient bulk upsert of multiple balances.

        Uses raw SQL for maximum throughput when processing many
        balance updates from a single mini-block.
        """
        if not balances:
            return 0

        from sqlalchemy import text
        # PostgreSQL INSERT ... ON CONFLICT with VALUES clause
        values_clauses = []
        for b in balances:
            values_clauses.append(
                f"('{b.user_address}', '{b.token_address}', '{b.token_symbol}', "
                f"{b.balance_raw}, {b.decimals}, {b.block_number}, NOW())"
            )
        sql = text(f"""
            INSERT INTO asset_balances (user_address, token_address, token_symbol, balance_raw, decimals, block_number, updated_at)
            VALUES {', '.join(values_clauses)}
            ON CONFLICT (user_address, token_address) DO UPDATE SET
                balance_raw = EXCLUDED.balance_raw,
                token_symbol = EXCLUDED.token_symbol,
                block_number = EXCLUDED.block_number,
                updated_at = EXCLUDED.updated_at
        """)
        result = await self._session.execute(sql)
        await self._session.commit()
        return len(balances)
```

---

## 5. Async Worker Pool Patterns

### 5.1 Production-Grade Worker Pool with Redis Queue

The worker pool processes execution plans from Redis. Each worker is an independent asyncio task
that pops from the queue, validates conditions, assembles transactions, and broadcasts.

```python
"""
app/execution/worker_pool.py — Async worker pool for transaction dispatch.

Workers consume ExecutionPayload JSON from Redis, validate trigger conditions,
assemble and sign transactions, and broadcast via eth_sendRawTransactionSync.

This is the REFLEX layer (<1ms target) — deterministic, no LLM calls.

Key design patterns:
- asyncio.TaskGroup for structured concurrency (Python 3.11+)
- asyncio.Semaphore for concurrency limits
- Exponential backoff on transient failures
- Clean shutdown via asyncio.Event
"""
from __future__ import annotations

import asyncio
import signal
import time
from typing import Any

import redis.asyncio as redis

from app.agents.dispatch import ExecutionPayload, ExecutionQueue
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger("megaeth.execution.worker_pool")


class TransactionExecutor:
    """Executes a single transaction plan. Deterministic, no LLM.

    In Phase 2, this integrates web3.py for signing and broadcasting.
    The web3.py integration is gated behind the Redis queue — agents
    never import web3.py directly.
    """

    def __init__(self, web3_provider=None) -> None:
        self._w3 = web3_provider  # web3.AsyncWeb3 instance (initialized lazily)

    async def execute(self, payload: ExecutionPayload) -> dict[str, Any]:
        """Execute a single transaction plan.

        Returns:
            {"status": "success", "tx_hash": "0x..."} or {"status": "failed", "error": "..."}
        """
        # Step 1: Validate trigger conditions
        if not self._check_conditions(payload.trigger_condition_schema):
            return {"status": "skipped", "reason": "conditions not met"}

        # Step 2: Validate slippage (hard cap at 1%)
        if payload.max_slippage_bps > 100:
            return {"status": "rejected", "reason": "slippage exceeds 1% hard cap"}

        # Step 3: Assemble and broadcast transaction
        # (web3.py integration goes here — not shown to keep this readable)
        # tx_hash = await self._w3.eth.send_raw_transaction(signed_tx)

        return {"status": "success", "tx_hash": "0x<placeholder>"}

    def _check_conditions(self, schema: dict[str, Any]) -> bool:
        """Check trigger conditions (price thresholds, time bounds, etc.)."""
        condition_type = schema.get("type", "")
        if condition_type == "price_threshold":
            # In production, query an oracle or cached price feed
            return True
        if condition_type == "always":
            return True
        return False


class WorkerPool:
    """Pool of async workers consuming from the Redis execution queue.

    Usage:
        pool = WorkerPool(worker_count=4,
                          redis_url=settings.redis_url)
        await pool.start()
        # ... workers run in background ...
        await pool.stop()

    Design:
    - Each worker is an independent asyncio task
    - Workers use BRPOP with timeout (non-blocking enough to check shutdown)
    - Semaphore limits concurrent transaction execution
    - Clean shutdown drains in-flight work, then exits
    """

    def __init__(
        self,
        worker_count: int = 4,
        redis_url: str = "",
        max_concurrent_tx: int = 10,
    ) -> None:
        self.worker_count = worker_count
        self.redis_url = redis_url or settings.redis_url
        self.max_concurrent_tx = max_concurrent_tx
        self._shutdown = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._semaphore = asyncio.Semaphore(max_concurrent_tx)
        self._redis: redis.Redis | None = None
        self._executor = TransactionExecutor()

    async def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def start(self) -> None:
        """Start all worker tasks."""
        logger.info("worker pool starting", worker_count=self.worker_count)
        for i in range(self.worker_count):
            task = asyncio.create_task(self._worker(i), name=f"executor-{i}")
            self._tasks.append(task)
        logger.info("worker pool started")

    async def stop(self) -> None:
        """Gracefully stop all workers."""
        logger.info("worker pool stopping")
        self._shutdown.set()

        # Wait for all workers to finish (with timeout)
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            logger.warning("worker pool shutdown timed out, cancelling")
            for t in self._tasks:
                t.cancel()

        if self._redis:
            await self._redis.close()

        logger.info("worker pool stopped")

    async def _worker(self, worker_id: int) -> None:
        """Single worker loop: pop from queue, execute, repeat."""
        r = await self._get_redis()
        queue = ExecutionQueue(r)

        logger.debug("worker started", worker_id=worker_id)

        while not self._shutdown.is_set():
            try:
                # BRPOP with 1s timeout — allows checking shutdown_event
                payload = await queue.dequeue(timeout=1.0)

                if payload is None:
                    continue  # No work available, re-check shutdown

                # Throttle concurrent executions via semaphore
                async with self._semaphore:
                    result = await self._executor.execute(payload)
                    if result["status"] == "success":
                        await queue.set_result(
                            payload.id, "completed", tx_hash=result.get("tx_hash", "")
                        )
                    else:
                        await queue.set_result(
                            payload.id, "skipped", error=result.get("reason", "")
                        )
                    logger.debug(
                        "execution completed",
                        worker_id=worker_id,
                        execution_id=payload.id,
                        status=result["status"],
                    )

            except Exception as e:
                logger.error(
                    "worker error",
                    worker_id=worker_id,
                    error=str(e),
                    exc_info=True,
                )
                # Backoff on persistent errors
                await asyncio.sleep(1.0)

        logger.debug("worker stopped", worker_id=worker_id)


async def main() -> None:
    """Entrypoint for the worker pool (standalone process)."""
    pool = WorkerPool(worker_count=4)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(pool.stop()))

    await pool.start()
    await pool._shutdown.wait()


if __name__ == "__main__":
    asyncio.run(main())
```

### 5.2 WorkerPool + Supervisor Integration

Following the existing supervisor pattern from `app/api/main.py`:

```python
"""
Add this to app/api/main.py supervise() function:

    from app.execution.worker_pool import WorkerPool

    # Add to the task list alongside ingestion_task, state_task, vector_task
    worker_pool = WorkerPool(worker_count=4)
    await worker_pool.start()
    tasks.append(asyncio.create_task(worker_pool._shutdown.wait()))
"""
```

---

## 6. Subgraph / GraphQL Query Patterns

### 6.1 gql Client with aiohttp Transport (Production Pattern)

The `gql` library already in `pyproject.toml` supports async HTTP transport, schema introspection,
and variable substitution. This is used by the `query_historical_subgraph` tool.

```python
"""
app/data/subgraph_client.py — Typed GraphQL client for blockchain subgraphs.

Provides type-safe queries with proper error handling, pagination,
and caching of the introspected schema.
"""
from __future__ import annotations

import asyncio
from typing import Any

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError


class SubgraphClient:
    """Async GraphQL client for The Graph hosted service or decentralized network.

    Caches the introspected schema on first connect. Supports pagination
    via first/skip arguments and variable substitution.
    """

    def __init__(self, url: str, api_key: str = "", timeout: float = 30.0):
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self._transport = AIOHTTPTransport(
            url=url,
            headers=headers,
            client_session_args={"timeout": timeout},
        )
        self._client = Client(
            transport=self._transport,
            fetch_schema_from_transport=True,  # Introspects schema on first connect
        )

    async def execute(
        self,
        query_str: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query with optional variables."""
        async with self._client as session:
            query = gql(query_str)
            try:
                result = await session.execute(query, variable_values=variables)
                return result
            except TransportQueryError as e:
                raise RuntimeError(f"GraphQL error: {e.errors}") from e

    async def close(self) -> None:
        """Close the HTTP session."""
        await self._transport.session.close()


# ---------------------------------------------------------------------------
# Example: Uniswap V3 subgraph query for token data
# ---------------------------------------------------------------------------
UNISWAP_V3_SUBGRAPH = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"


async def fetch_token_price_history(
    token_address: str,
    days: int = 7,
) -> list[dict[str, Any]]:
    """Fetch daily price data for a token from Uniswap V3 subgraph."""
    client = SubgraphClient(UNISWAP_V3_SUBGRAPH)
    try:
        result = await client.execute("""
            query TokenDayData($token: String!, $days: Int!) {
                tokenDayDatas(
                    first: $days,
                    orderBy: date,
                    orderDirection: desc,
                    where: { token: $token }
                ) {
                    date
                    priceUSD
                    volumeUSD
                    totalValueLockedUSD
                }
            }
        """, {"token": token_address.lower(), "days": days})
        return result.get("tokenDayDatas", [])
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Example: MegaETH-specific subgraph query (if available on The Graph)
# ---------------------------------------------------------------------------
MEGAETH_SUBGRAPH = "https://api.studio.thegraph.com/query/XXXXX/megaeth-testnet/v0.0.1"


async def fetch_recent_swaps(
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Fetch recent swaps from a MegaETH subgraph."""
    client = SubgraphClient(MEGAETH_SUBGRAPH)
    try:
        result = await client.execute("""
            query RecentSwaps($limit: Int!) {
                swaps(first: $limit, orderBy: timestamp, orderDirection: desc) {
                    id
                    tokenIn { symbol address }
                    tokenOut { symbol address }
                    amountIn
                    amountOut
                    timestamp
                    sender
                }
            }
        """, {"limit": limit})
        return result.get("swaps", [])
    finally:
        await client.close()
```

---

## 7. Qdrant Vector Similarity Search with Filters

### 7.1 Search with Payload Filters (Production Pattern)

The existing `QdrantManager` uses the sync `QdrantClient`. For Phase 2, add async-compatible
search with compound filters (AND/OR/must/should) and metadata-based scoping.

```python
"""
app/vector/search_service.py — Async semantic search over indexed mini-blocks.

Extends the existing QdrantManager with filter-based search for
Phase 2 cognitive memory retrieval.
"""
from __future__ import annotations

from typing import Any

from qdrant_client import QdrantClient as SyncQdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchAny,
    MatchValue,
    PointStruct,
    Range,
    ScoredPoint,
    VectorParams,
)

from app.core.config import settings
from app.core.logging import get_logger
from app.vector.embedder import CohereEmbedder

logger = get_logger("megaeth.vector.search_service")


class VectorSearchService:
    """Semantic search service for cognitive memory retrieval.

    Wraps Qdrant with filter-based similarity search. Used by the
    ``search_cognitive_memory`` LangGraph tool.

    Filter patterns:
    - Scoping to a user: Filter(must=[FieldCondition(key="user_id", match=MatchValue(value="0x..."))])
    - Scoping to block range: Filter(must=[FieldCondition(key="block_number", range=Range(gte=1000, lte=2000))])
    - Multiple conditions: Filter(must=[...], must_not=[...], should=[...])
    """

    COLLECTION_NAME = "megaeth_blocks"
    MEMORY_COLLECTION = "megaeth_agent_memory"

    def __init__(self, qdrant_client: SyncQdrantClient | None = None):
        self._client = qdrant_client or SyncQdrantClient(
            url=settings.qdrant_url, timeout=30
        )
        self._embedder = CohereEmbedder()

    def initialize(self) -> None:
        """Ensure collections exist."""
        self._ensure_collection(self.COLLECTION_NAME,
                                size=settings.embedding_dimensions)
        self._ensure_collection(self.MEMORY_COLLECTION,
                                size=settings.embedding_dimensions)

    def _ensure_collection(self, name: str, size: int) -> None:
        existing = [c.name for c in self._client.get_collections().collections]
        if name not in existing:
            self._client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=size, distance=Distance.COSINE),
            )
            logger.info("collection created", collection=name)

    async def search(
        self,
        query_text: str,
        limit: int = 10,
        filters: dict[str, Any] | None = None,
        score_threshold: float = 0.7,
        collection: str = "",
    ) -> list[ScoredPoint]:
        """Semantic search with optional metadata filters.

        Args:
            query_text: Natural language query.
            limit: Max results to return.
            filters: Dict-based filter spec. Examples:
                {"user_address": "0x..."}  — exact match
                {"block_number": {"gte": 1000, "lte": 2000}}  — range
                {"token_symbol": ["USDM", "WETH"]}  — match any
            score_threshold: Minimum cosine similarity score.
            collection: Target collection (defaults to megaeth_blocks).
        """
        embedding = await self._embedder.embed(query_text)

        qdrant_filter = self._build_filter(filters) if filters else None
        collection_name = collection or self.COLLECTION_NAME

        # QdrantClient.search() is sync — wrap in run_in_executor for async compatibility
        import asyncio
        loop = asyncio.get_running_loop()
        results = await loop.run_in_executor(
            None,
            lambda: self._client.search(
                collection_name=collection_name,
                query_vector=embedding,
                query_filter=qdrant_filter,
                limit=limit,
                score_threshold=score_threshold,
                with_payload=True,
            ),
        )
        return results

    def _build_filter(self, filters: dict[str, Any]) -> Filter:
        """Convert a simple dict filter spec to a Qdrant Filter object.

        Supported dict formats:
        - {"key": "value"} -> FieldCondition(key=key, match=MatchValue(value=value))
        - {"key": {"gte": min, "lte": max}} -> FieldCondition(key=key, range=Range(...))
        - {"key": ["val1", "val2"]} -> FieldCondition(key=key, match=MatchAny(any=[...]))
        """
        conditions = []
        for key, value in filters.items():
            if isinstance(value, dict) and ("gte" in value or "lte" in value):
                conditions.append(
                    FieldCondition(key=key, range=Range(**value))
                )
            elif isinstance(value, list):
                conditions.append(
                    FieldCondition(key=key, match=MatchAny(any=value))
                )
            else:
                conditions.append(
                    FieldCondition(key=key, match=MatchValue(value=value))
                )
        return Filter(must=conditions) if conditions else Filter()

    def index_memory(
        self,
        point_id: str,
        text: str,
        embedding: list[float],
        metadata: dict[str, Any],
    ) -> None:
        """Store a memory entry in the agent memory collection."""
        point = PointStruct(
            id=point_id,
            vector=embedding,
            payload={"text": text, **metadata},
        )
        self._client.upsert(
            collection_name=self.MEMORY_COLLECTION,
            points=[point],
        )

    def search_memory(
        self,
        vector: list[float],
        limit: int = 5,
        user_address: str = "",
    ) -> list[ScoredPoint]:
        """Search agent memory with optional user scoping."""
        filter_obj = None
        if user_address:
            filter_obj = Filter(
                must=[FieldCondition(key="user_address", match=MatchValue(value=user_address))]
            )

        return self._client.search(
            collection_name=self.MEMORY_COLLECTION,
            query_vector=vector,
            query_filter=filter_obj,
            limit=limit,
            with_payload=True,
        )

    def count(self, collection: str = "") -> int:
        result = self._client.count(collection_name=collection or self.COLLECTION_NAME)
        return result.count

    def close(self) -> None:
        self._client.close()
```

---

## Putting It All Together — Phase 2 Supervisor

The updated `supervise()` function in `app/api/main.py` that launches all Phase 2 components:

```python
"""
app/api/main.py — Updated supervisor with Phase 2 agent + worker pool.

Add these tasks alongside the existing Phase 1 components:
- Agent graph (FastAPI routes invoke it directly — no task needed)
- Worker pool (4 async workers consuming execution plans from Redis)

The full task list becomes:
1. FastAPI server (uvicorn)
2. Ingestion daemon (WebSocket -> Redis)
3. State tracker worker (Redis -> PostgreSQL)
4. Vector indexer worker (Redis -> Qdrant)
5. [NEW] Execution worker pool (Redis -> transaction dispatch)
"""
# In the supervise() function, add:

# from app.execution.worker_pool import WorkerPool
# worker_pool = WorkerPool(worker_count=4)
# await worker_pool.start()
# # Track the shutdown event
# shutdown_tracker = asyncio.create_task(worker_pool._shutdown.wait())
# tasks.append(shutdown_tracker)
```

---

## Key Architecture Decisions

| Decision | Rationale |
|----------|-----------|
| Agents write JSON to Redis, never call web3.py | Enforces Strategy/Reflex Split; agents do cognition, workers execute |
| `with_structured_output()` for intent classification | Forces LLM into typed output; no regex parsing of free text |
| Composite PKs for balance tables | Enables efficient upserts and avoids UUID overhead |
| `asyncio.Semaphore` for worker concurrency limits | Prevents thundering herd on RPC endpoints |
| `fetch_schema_from_transport=True` for gql | Eliminates manual schema maintenance; validates queries at connect time |
| Qdrant filters via `Filter(must=[...])` pattern | Composable, type-safe metadata filtering without raw JSON |
| `MemorySaver` for dev, PostgreSQL checkpointer for prod | Dev gets zero-config persistence; prod gets durable checkpoints |
| `async with Client(...) as session` for gql | Auto-closes connections; prevents resource leaks in long-running agents |

---

## References

- LangGraph docs: `https://reference.langchain.com/python/langgraph/`
- LangGraph StateGraph API: `StateGraph(state_schema=State, context_schema=Context)`
- Qdrant Python client: `https://github.com/qdrant/qdrant-client`
- gql library: `https://github.com/graphql-python/gql`
- SQLModel async: `create_async_engine`, `async_sessionmaker`, `AsyncSession`
- Existing Phase 1: `app/streams/buffer.py`, `app/streams/consumer.py`, `app/state/worker.py`, `app/vector/worker.py`
- AGENTS.md: Strategy/Reflex Split, safety boundaries, transaction dispatch protocol
