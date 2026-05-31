# MegaETH Projects

**MegaETH development skill suite** for AI agents, plus a **production-ready event-driven ingestion pipeline** (Phase 1) for autonomous agent infrastructure on MegaETH.

## Overview

This repository contains:
- **Phase 1 pipeline** — A decoupled, event-driven ingestion system that connects to MegaETH or a local Foundry Anvil node, buffers real-time block data in Redis Streams, persists to PostgreSQL, and indexes into Qdrant for vector similarity search.
- **MegaETH development skill** — 26 knowledge files covering Foundry setup, smart contract patterns, wallet operations, x402 payments, and more.

## Architecture

```
Anvil / MegaETH WebSocket  →  Ingestion Daemon  →  Redis Stream Buffer
                                                      ├── PostgreSQL State Tracker
                                                      └── Qdrant Vector Indexer
```

| Component | Role | Key files |
|-----------|------|-----------|
| **Ingestion Daemon** | WebSocket → Redis XADD, 30s keepalive, exponential backoff | `app/ingestion/daemon.py` |
| **Redis Stream Buffer** | Consumer groups, XREADGROUP, XACK, XAUTOCLAIM, dead-letter | `app/streams/buffer.py`, `consumer.py` |
| **PostgreSQL State Tracker** | Redis Stream → asyncpg upserts (ON CONFLICT DO UPDATE) | `app/state/worker.py`, `repository.py` |
| **Qdrant Vector Indexer** | Text summaries → cohere-embed (1024-dim) → Qdrant | `app/vector/worker.py`, `embedder.py` |
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

Single supervisor (runs all 4 components concurrently):

```bash
uv run python -m app.api.main
```

Or run components individually:

```bash
uv run python -m app.ingestion.daemon   # WebSocket → Redis Stream
uv run python -m app.state.worker       # Redis Stream → PostgreSQL
uv run python -m app.vector.worker      # Redis Stream → Qdrant
```

## Testing

```bash
# Full test suite (49 tests)
uv run -m pytest tests/ -v

# Soak test (30s at 100 blocks/sec)
SOAK_DURATION_SEC=30 uv run -m pytest tests/soak/ -v --soak -s

# Chaos tests (requires Docker)
SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_chaos.py -v -s
```

## Project Structure

```
app/
├── api/              FastAPI health/metrics endpoints
├── core/             Config, logging, metrics counters
├── ingestion/        WebSocket daemon + backoff
├── schemas/          Pydantic data contracts
├── state/            PostgreSQL worker + repository
├── streams/          Redis Stream buffer + consumer
└── vector/           Qdrant indexer + embedder + summarizer
tests/
├── unit/             23 schema + 4 daemon unit tests
├── integration/      22 integration tests (pipeline, chaos, state, streams)
└── soak/             Endurance test framework
```

## License

MIT
