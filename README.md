# megaETH-Prism

[![GitHub](https://img.shields.io/badge/GitHub-ParzivalXIII/megaETH--Prism-8A2BE2)](https://github.com/ParzivalXIII/megaETH-Prism)

**MegaETH development skill suite** for AI agents, plus a **production-ready event-driven orchestration system** for autonomous agent infrastructure on MegaETH.

## Overview

This repository contains:

- **Block ingestion pipeline** — A decoupled, event-driven ingestion system that connects to MegaETH or a local Foundry Anvil node, buffers real-time block data in Redis Streams, persists to PostgreSQL, and indexes into Qdrant for vector similarity search.
- **Transaction execution infrastructure** — Execution intent schemas, real-time asset balance tracking via ERC-20 Transfer event extraction, and an async worker pool that signs and broadcasts transactions to MegaETH.
- **AI agent layer** — A two-agent LangGraph StateGraph (Market Intelligence → Portfolio Router) that runs as a background polling loop, queries Qdrant memory and PostgreSQL state, produces typed `ExecutionPayload` intents, and dispatches them to the execution stream. Tools are executed via LangGraph `ToolNode`, with calldata safety enforced by a function-selector allowlist.
- **MegaETH development skill** — 26 knowledge files covering Foundry setup, smart contract patterns, wallet operations, x402 payments, and more.

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                    LANGGRAPH AI AGENT LAYER                          │
 │                                                                     │
 │  ┌──────────────────────┐    ┌──────────────────────────────┐       │
 │  │  Market Intelligence  │◄──►│   search_cognitive_memory    │       │
 │  │      (LLM Node)       │    │   query_historical_subgraph  │       │
 │  └──────────┬───────────┘    └──────────────────────────────┘       │
 │             │ (conditional: tool_calls? → ToolNode : → router)      │
 │             ▼                                                        │
 │  ┌──────────────────────┐                                           │
 │  │   Market ToolNode    │  executes tools, returns results           │
 │  └──────────┬───────────┘                                           │
 │             ▼                                                        │
 │  ┌──────────────────────┐    ┌──────────────────────────────┐       │
 │  │  Portfolio Router    │◄──►│   query_portfolio            │       │
 │  │      (LLM Node)       │    │   query_balance              │       │
 │  └──────────┬───────────┘    │   search_cognitive_memory    │       │
 │             │                │   dispatch_execution          │       │
 │             ▼                └──────────────────────────────┘       │
 │  ┌──────────────────────┐                                           │
 │  │ Portfolio ToolNode   │  executes dispatch, enqueues payload      │
 │  └──────────┬───────────┘                                           │
 │             │ (ExecutionPayload JSON)                                │
 └─────────────┼───────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    REDIS EXECUTION STREAM                             │
│           `megaeth:stream:executions` — XADD / XREADGROUP            │
│           Consumer group: `megaeth:workers:executors`                │
│           Results stored as Redis HASH with 24h TTL                  │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │ (consumes via async worker pool)
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      ASYNC WORKER POOL                                │
│  TransactionExecutor: validates conditions, signs, broadcasts via    │
│  eth_sendRawTransactionSync / eth_sendRawTransaction                 │
│  Single-worker (PRIVATE_KEY) with asyncio.Semaphore concurrency      │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │ (eth_sendRawTransaction[Sync])
                                 ▼
                       [ MegaETH Sequencer / Node ]

Anvil / MegaETH        ──►  Ingestion Daemon  ──►  Redis Stream Buffer
 WebSocket                                       ┌─── PostgreSQL State Tracker
                                                  ├─── Qdrant Vector Indexer
                                                  └─── Balance Tracker Worker
                                                       └── ERC-20 Transfer parser
                                                           └── AssetBalance upsert
```

| Component | Role | Key files |
|-----------|------|-----------|
| **Ingestion Daemon** | WebSocket → Redis XADD, 30s keepalive, exponential backoff | `app/ingestion/daemon.py` |
| **Redis Stream Buffer** | Consumer groups, XREADGROUP, XACK, XAUTOCLAIM, dead-letter | `app/streams/buffer.py`, `consumer.py` |
| **PostgreSQL State Tracker** | Redis Stream → asyncpg upserts (ON CONFLICT DO UPDATE) | `app/state/worker.py`, `repository.py` |
| **Qdrant Vector Indexer** | Text summaries → cohere-embed (1024-dim) → Qdrant | `app/vector/worker.py`, `embedder.py` |
| **Execution Queue** | Redis STREAM queue for agent intents (XADD/XREADGROUP/XACK) | `app/agents/dispatch.py` |
| **ERC-20 Transfer Parser** | Extracts Transfer events from mini-block receipt logs | `app/state/transfer_parser.py` |
| **Asset Balance Repository** | PostgreSQL upsert for per-user asset balances | `app/state/balance_repository.py` |
| **Balance Tracker Worker** | Consumes from the ingestion stream → extracts transfers → upserts | `app/state/balance_worker.py` |
| **Transaction Executor** | Signs & broadcasts transactions via MegaETH RPC | `app/agents/dispatcher.py` |
| **Worker Pool** | Async workers consuming execution stream, single-worker mode | `app/agents/dispatcher.py` |
| **Market Intelligence Agent** | LangGraph LLM node — analyses market via Qdrant + subgraph tools | `app/agents/graph.py` |
| **Portfolio Router Agent** | LangGraph LLM node — evaluates portfolio, dispatches payloads | `app/agents/graph.py` |
| **AgentRunner** | Background polling loop with idle backoff, single-invocation lock | `app/agents/runner.py` |
| **Subgraph Client** | Async gql client with fixed queries (Uniswap V3 / Envio) | `app/agents/tools/subgraph.py` |
| **Portfolio Query Tool** | Queries PostgreSQL asset balances via repository | `app/agents/tools/local_state.py` |
| **Cognitive Memory Tool** | Semantic search over Qdrant (sync wrapped in `to_thread`) | `app/agents/tools/memory.py` |
| **Dispatch Tool** | Enqueues ExecutionPayload with calldata safety allowlist | `app/agents/tools/dispatch.py` |
| **Agent API** | `POST /agent/invoke` for manual/ad-hoc invocation | `app/api/routes/agent.py` |
| **Health & Metrics API** | FastAPI `/health/live`, `/health/ready`, `/metrics` | `app/api/main.py`, `routes/` |

## Quick Start

### 0. Clone the repository

```bash
git clone https://github.com/ParzivalXIII/megaETH-Prism.git
cd megaETH-Prism
```

### Prerequisites

- Python 3.11+ with `uv` installed
- Docker and Docker Compose v5+
- Foundry (for Anvil)

### 1. Start a local Anvil node

Run this in a separate terminal with MegaETH-optimized parameters:

```bash
anvil \
  --chain-id 6343 \
  --gas-price 1000000 \
  --block-gas-limit 10000000000 \
  --block-time 1 \
  --accounts 10 \
  --balance 10000
```

The daemon auto-detects Anvil (chain ID 6343 → `newHeads` subscription for blocks). To connect to the real MegaETH testnet instead, set `WS_URL=wss://carrot.megaeth.com/ws CHAIN_ID=6343 SUBSCRIPTION_TYPE=miniBlocks` in `.env`.

### 2. Start infrastructure services

```bash
docker compose up -d
```

Launches PostgreSQL 16, Redis 7.2, Qdrant, and cohere-embed with health checks.

### 3. Set environment variables

Copy `.env.example` to `.env` and set:

```bash
# Required for the AI agent layer (get yours at https://opencode.ai/auth)
OPENCODE_GO_API_KEY=opencode-go-<your-key>

# Optional: override LLM model, subgraph type, polling interval
LLM_MODEL=deepseek-v4-flash
SUBGRAPH_TYPE=uniswap_v3
AGENT_POLL_INTERVAL_SEC=60
```

### 4. Run the pipeline

Single supervisor (runs all 7 components concurrently):

```bash
uv run python -m app.api.main
```

Or run components individually:

```bash
uv run python -m app.ingestion.daemon   # WebSocket → Redis Stream
uv run python -m app.state.worker       # Redis Stream → PostgreSQL
uv run python -m app.vector.worker      # Redis Stream → Qdrant
```

These additional components are started automatically by the supervisor, or can run standalone:

```bash
# Balance tracker (consumes from the ingestion stream, extracts Transfer events)
uv run python -c "
import asyncio
from app.state.balance_worker import BalanceTrackerWorker
async def main():
    w = BalanceTrackerWorker(); await w.start(); await w.shutdown_event.wait()
asyncio.run(main())
"

# Worker pool (consumes execution stream, signs & broadcasts)
# Requires PRIVATE_KEY env var (Anvil account #0 for local dev):
#   0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
uv run python -c "
import asyncio
from app.agents.dispatcher import WorkerPool
async def main():
    w = WorkerPool(); await w.start(); await w.shutdown_event.wait()
asyncio.run(main())
"
```

The AI agent layer is started by the supervisor if `OPENCODE_GO_API_KEY` is set:

```bash
# Invoke the agent manually via API (requires supervisor running)
curl -X POST http://127.0.0.1:8080/agent/invoke \
  -H "Content-Type: application/json" \
  -d '{"user_address": "0x402085c248EeA27D92E8b30b2C58ed07f9E20001"}'
```

### 5. Enqueue an execution intent (manual test)

```bash
uv run python -c "
import asyncio, json, redis.asynced as redis
from app.schemas.intent import ExecutionPayload, TriggerCondition
from app.agents.dispatch import ExecutionQueue
async def main():
    r = redis.from_url('redis://127.0.0.1:6379/0', decode_responses=True)
    q = ExecutionQueue(r)
    await q.initialize()
    payload = ExecutionPayload(
        target_contract='0x402085c248EeA27D92E8b30b2C58ed07f9E20001',
        call_data='0x',
        trigger_condition=TriggerCondition(condition_type='always'),
    )
    eid = await q.enqueue(payload)
    print(f'Enqueued: {eid}')
    # Poll for result
    import time; time.sleep(3)
    result = await q.get_result(payload.id)
    print(f'Result: {json.dumps(result, indent=2)}')
    await r.close()
asyncio.run(main())
"
```

## Testing

```bash
# Full test suite (142+ tests)
uv run -m pytest tests/ -v

# Agent layer unit tests (state, tools, calldata safety, graph nodes, triggers)
uv run -m pytest tests/unit/test_agent_*.py tests/unit/test_calldata_safety.py tests/unit/test_graph_nodes.py tests/unit/test_triggers.py -v
uv run -m pytest tests/integration/test_agent_*.py -v

# Calldata safety tests (6 scenarios: safe, dangerous, unknown, wildcard, slippage)
uv run -m pytest tests/unit/test_calldata_safety.py -v

# Graph node tests (routing, tool execution, error handling, recursion guard)
uv run -m pytest tests/unit/test_graph_nodes.py -v

# Trigger condition evaluators (price_threshold, time_bound)
uv run -m pytest tests/unit/test_triggers.py -v

# Soak test (30s at 100 blocks/sec)
SOAK_DURATION_SEC=30 uv run -m pytest tests/soak/ -v --soak -s

# Chaos tests (requires Docker)
SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_chaos.py -v -s

# Execution and balance integration tests
uv run -m pytest tests/integration/test_dispatcher.py tests/integration/test_asset_balances.py tests/integration/test_balance_worker.py -v
```

## Project Structure

```
app/
├── agents/           AI agent infrastructure + execution layer
│   ├── __init__.py              Public API exports
│   ├── dispatch.py              ExecutionQueue (Redis STREAM)
│   ├── dispatcher.py            TransactionExecutor + WorkerPool + TriggerEvaluators
│   ├── state.py                 AgentState + AgentContext TypedDicts
│   ├── system_prompt.py         Agent system prompt constant
│   ├── graph.py                 LangGraph StateGraph (Market Intelligence →
│   │                            Portfolio Router) with ToolNodes
│   ├── runner.py                AgentRunner background polling loop
│   └── tools/
│       ├── __init__.py          Tool re-exports
│       ├── dispatch.py          ExecutionPayload enqueue + calldata safety
│       ├── local_state.py       Portfolio & balance query tools
│       ├── memory.py            Cognitive memory search via Qdrant
│       └── subgraph.py          SubgraphClient with fixed queries
├── api/              FastAPI endpoints + dependencies
│   ├── dependencies.py          DI: DB sessions, Redis, queues, agent graph
│   ├── main.py                  Supervisor — runs all 7 components concurrently
│   └── routes/
│       ├── agent.py             POST /agent/invoke (manual invocation)
│       ├── health.py            Health checks
│       └── metrics.py           Prometheus /metrics (incl. agent counters)
├── core/             Config, logging, metrics counters
│   ├── config.py                Settings (LLM, subgraph, polling)
│   └── metrics.py               Counters (agent invocations, tool calls)
├── ingestion/        WebSocket daemon + backoff
├── schemas/          Pydantic data contracts
│   ├── intent.py     ExecutionPayload + TriggerCondition
│   ├── mini_block.py MiniBlock payload
│   ├── state.py      MiniBlockRecord SQLModel
│   ├── streams.py    StreamEntry model
│   └── vector.py     VectorPayload model
├── state/            PostgreSQL workers + repositories
│   ├── balance_repository.py  AssetBalance upsert/query
│   ├── balance_worker.py      BalanceTrackerWorker
│   ├── models.py              AssetBalance + TokenTransfer tables
│   ├── repository.py          MiniBlockRecord CRUD
│   ├── transfer_parser.py     ERC-20 Transfer event extractor
│   └── worker.py              StateTrackerWorker
├── streams/          Redis Stream buffer + consumer
└── vector/           Qdrant indexer + embedder + summarizer
tests/
├── unit/             93 unit tests (schemas, parsers, daemon, state, agents
│                     tools, calldata safety, graph nodes, triggers)
├── integration/      49 integration tests (pipeline, chaos, state, streams,
│                     dispatcher, balances, agent graph, runner, API)
└── soak/             Endurance test framework
scripts/
└── validate_llm.py   LLM structured output validation POC
research/
├── execution-patterns.md   Execution infrastructure design research
└── agent-research.md       OpenCode Go API + Envio HyperIndex research
```

## Repository

[https://github.com/ParzivalXIII/megaETH-Prism](https://github.com/ParzivalXIII/megaETH-Prism)

## License

MIT
