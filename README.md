# MegaETH Projects

**MegaETH development skill suite** for AI agents, plus a **production-ready event-driven orchestration system** (Phase 1 + Phase 2) for autonomous agent infrastructure on MegaETH.

## Overview

This repository contains:
- **Phase 1 pipeline** — A decoupled, event-driven ingestion system that connects to MegaETH or a local Foundry Anvil node, buffers real-time block data in Redis Streams, persists to PostgreSQL, and indexes into Qdrant for vector similarity search.
- **Phase 2 cognitive infrastructure** — Execution intent schemas, real-time asset balance tracking via ERC-20 Transfer event extraction, and an async worker pool that signs and broadcasts transactions to MegaETH. This bridges the gap between the ultra-fast reflex layer and future LangGraph cognitive agents (Phase 3).
- **MegaETH development skill** — 26 knowledge files covering Foundry setup, smart contract patterns, wallet operations, x402 payments, and more.

## Architecture

```
                            ┌──────────────────────────────────────────┐
                            │      LANggRAPH COGNITION (Phase 3)       │
                            │    Uses historical + Qdrant memories     │
                            │    to evaluate market status & intent    │
                            └────────────────┬─────────────────────────┘
                                             │ (writes ExecutionPayload JSON)
                                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  REDIS EXECUTION STREAM (Phase 2)                     │
│           `megaeth:stream:executions` — XADD / XREADGROUP            │
│           Consumer group: `megaeth:workers:executors`                │
│           Results stored as Redis HASH with 24h TTL                  │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │ (consumes via async worker pool)
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     ASYNC WORKER POOL (Phase 2)                       │
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
                                                  └─── BalanceTrackerWorker (Phase 2)
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
| **Balance Tracker Worker** | Consumes from Phase 1 stream → extracts transfers → upserts | `app/state/balance_worker.py` |
| **Transaction Executor** | Signs & broadcasts transactions via MegaETH RPC | `app/agents/dispatcher.py` |
| **Worker Pool** | Async workers consuming execution stream, single-worker mode | `app/agents/dispatcher.py` |
| **Health & Metrics API** | FastAPI `/health/live`, `/health/ready`, `/metrics` | `app/api/main.py`, `routes/` |

## Quick Start

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

### 3. Run the pipeline

Single supervisor (runs all 6 components concurrently):

```bash
uv run python -m app.api.main
```

Or run components individually:

```bash
uv run python -m app.ingestion.daemon   # WebSocket → Redis Stream
uv run python -m app.state.worker       # Redis Stream → PostgreSQL
uv run python -m app.vector.worker      # Redis Stream → Qdrant
```

**Phase 2 components** are started automatically by the supervisor, or can run standalone:

```bash
# Balance tracker (consumes from Phase 1 stream, extracts Transfer events)
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

### 4. Enqueue an execution intent (manual test)

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
# Full test suite (97 tests)
uv run -m pytest tests/ -v

# Soak test (30s at 100 blocks/sec)
SOAK_DURATION_SEC=30 uv run -m pytest tests/soak/ -v --soak -s

# Chaos tests (requires Docker)
SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_chaos.py -v -s

# Phase 2 integration tests only
uv run -m pytest tests/integration/test_dispatcher.py tests/integration/test_asset_balances.py tests/integration/test_balance_worker.py -v
```

## Project Structure

```
app/
├── agents/           Phase 2 worker pool + execution queue
│   ├── dispatch.py   ExecutionQueue (Redis STREAM)
│   └── dispatcher.py TransactionExecutor + WorkerPool
├── api/              FastAPI health/metrics endpoints + dependencies
│   └── dependencies.py  Phase 2 dependency injection (DB, Redis, queues)
├── core/             Config, logging, metrics counters
├── ingestion/        WebSocket daemon + backoff
├── schemas/          Pydantic data contracts
│   ├── intent.py     ExecutionPayload + TriggerCondition (Phase 2)
│   ├── mini_block.py MiniBlock payload (Phase 1)
│   ├── state.py      MiniBlockRecord SQLModel (Phase 1)
│   ├── streams.py    StreamEntry model (Phase 1)
│   └── vector.py     VectorPayload model (Phase 1)
├── state/            PostgreSQL workers + repositories
│   ├── balance_repository.py  AssetBalance upsert/query (Phase 2)
│   ├── balance_worker.py      BalanceTrackerWorker (Phase 2)
│   ├── models.py              AssetBalance + TokenTransfer tables (Phase 2)
│   ├── repository.py          MiniBlockRecord CRUD (Phase 1)
│   ├── transfer_parser.py     ERC-20 Transfer event extractor (Phase 2)
│   └── worker.py              StateTrackerWorker (Phase 1)
├── streams/          Redis Stream buffer + consumer
└── vector/           Qdrant indexer + embedder + summarizer
tests/
├── unit/             30 schema + parser + daemon unit tests
├── integration/      47 integration tests (pipeline, chaos, state,
│                     streams, dispatcher, balances, balance worker)
└── soak/             Endurance test framework
```

## License

MIT
