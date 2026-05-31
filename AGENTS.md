# AGENTS.md — MegaETH Projects

## What this repo is

A **MegaETH development skill suite** for AI agents, plus the **Phase 1 event-driven orchestration system** for autonomous agent infrastructure on MegaETH. Contains executable ingestion, storage, and indexing pipeline, plus the installed OpenCode skill set.

## Orientation

| What | Where |
|------|-------|
| MegaETH dev skill (26 files) | `.opencode/skills/megaeth-developer/` |
| Python project config | `pyproject.toml` |
| Locked dependencies | `uv.lock` |
| **Phase 1 orchestration code** | `app/` |
| Integration tests (45 tests) | `tests/` |
| Docker Compose (4 infra services) | `docker-compose.yml` |
| Entrypoint (supervisor) | `app/api/main.py` |
| Python version | 3.11 (`uv` managed) |

The skill at `.opencode/skills/megaeth-developer/SKILL.md` is the **primary knowledge base** for building on MegaETH. Read it first for any MegaETH task.

## Phase 1: Event-Driven Orchestration System (LIVE)

The ingestion-to-indexing pipeline is built and running. Four decoupled components connected via Redis Streams:

```
MegaETH / Anvil WebSocket  →  Ingestion Daemon  →  Redis Stream Buffer
                                                       ├── PostgreSQL State Tracker
                                                       └── Qdrant Vector Indexer
```

### Component details

| Component | Role | Key files |
|-----------|------|-----------|
| **Ingestion Daemon** | WebSocket → Redis XADD, 30s keepalive, exponential backoff, structural Pydantic validation | `app/ingestion/daemon.py` |
| **Redis Stream Buffer** | Consumer groups, XREADGROUP, XACK, XAUTOCLAIM, dead-letter routing | `app/streams/buffer.py`, `consumer.py` |
| **PostgreSQL State Tracker** | Redis Stream → SQLModel/asyncpg upserts, ON CONFLICT DO UPDATE | `app/state/worker.py`, `repository.py` |
| **Qdrant Vector Indexer** | Text summaries → cohere-embed (1024-dim) → Qdrant cosine search | `app/vector/worker.py`, `embedder.py` |
| **Health & Metrics API** | FastAPI `/health/live`, `/health/ready`, `/metrics` (Prometheus) | `app/api/main.py`, `routes/` |

### Key pipeline metrics
- **Ingestion**: ~28 mini-blocks/sec on testnet, ~10 blocks/10s on Anvil with `--block-time 1`, XADD in ~1ms
- **Storage**: PostgreSQL upsert via `session.merge()` (idempotent)
- **Embedding**: Manual dict-based cache (NOT lru_cache on async), degraded mode if cohere-embed unavailable
- **Buffer**: MAXLEN ~500K with approximate trimming, 30s XAUTOCLAIM recovery
- **Poison pills**: 3-retry dead-letter stream (`megaeth:raw:miniBlocks:dead`)

### Data contracts (app/schemas/)

```
MiniBlockPayload — matches actual MegaETH API (hex strings, no block_hash)
StreamEntry      — Redis stream entry with canonical field name constants
MiniBlockRecord  — SQLModel table, composite PK (block_number, index)
VectorPayload    — Qdrant point, 1024-dim embedding
```

### Startup
First, start an Anvil node with MegaETH-optimized parameters in a separate terminal:

```bash
anvil \
  --chain-id 6343 \
  --gas-price 1000000 \
  --block-gas-limit 10000000000 \
  --block-time 1 \
  --accounts 10 \
  --balance 10000
```

Then start infra services and the pipeline:

```bash
# Start all infra services
docker compose up -d

# Run ingestion + workers + API (supervisor — runs all 4 tasks)
uv run python -m app.api.main

# Or run individual components:
uv run python -m app.ingestion.daemon
uv run python -m app.state.worker
uv run python -m app.vector.worker

# Override RPC to connect to different backends:
WS_URL=wss://carrot.megaeth.com/ws CHAIN_ID=6343 SUBSCRIPTION_TYPE=miniBlocks uv run python -m app.ingestion.daemon
```

The daemon auto-detects the backend: on `chain_id=6343` with `SUBSCRIPTION_TYPE=auto`, it tries `miniBlocks` (MegaETH) first, falls back to `newHeads` (standard EVM/Anvil). The `.env` defaults to `ws://127.0.0.1:8545` for the local Anvil node.

### Testing
```bash
# Full test suite (49 tests)
uv run -m pytest tests/ -v

# Soak test (30s at 100 blocks/sec)
SOAK_DURATION_SEC=30 uv run -m pytest tests/soak/ -v --soak -s

# Chaos tests (requires Docker, skips by default)
SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_chaos.py -v -s
```

## Architecture: Strategy/Reflex Split (target design)

Agents on a 10ms-block-time L2 must never execute blocking tasks inside transaction pipelines.

| Path | Latency | Implementation | Responsibility |
|------|---------|----------------|----------------|
| **Cognition / Strategy** | 500–3000ms | LangGraph state machines | Macro analysis, LP deviation forecasts, intent pathways, risk bounds |
| **Execution / Reflexes** | <1ms | Async Python relay workers (Redis Streams) | Deterministic target eval against block stream, instant signing |

### Agent roles (LangGraph)

- **Intent Router** — decomposes user commands into multi-hop intent topologies. Tools: `query_local_state` (PostgreSQL), `route_intent_to_analyzer`. Output: deterministic JSON execution definitions.
- **Portfolio & Market Analysis Agent** — evaluates market data, calculates risk ceilings. Tools: `query_historical_subgraph` (async GraphQL via `gql`), `search_cognitive_memory` (Qdrant vector similarity).

### Transaction Dispatch Protocol

1. LangGraph agent writes JSON to Redis execution queue (never calls `web3.py` directly).
2. Payload: `target_contract`, `call_data`, `max_gas_price`, `trigger_condition_schema`.
3. Async Python worker pool assembles and broadcasts.
4. Workers are the only code that touches private keys.

### Safety boundaries

- **Slippage**: hard-coded max 1%. Human override required beyond that.
- **Gas spikes**: workers drop non-essential tx without blocking the agent loop.

## MegaETH cheat sheet (reproduced from skill)

| Network | Chain ID | RPC | Explorer |
|---------|----------|-----|----------|
| Mainnet | 4326 | `https://mainnet.megaeth.com/rpc` | `https://mega.etherscan.io` |
| Testnet | 6343 | `https://carrot.megaeth.com/rpc` | `https://megaeth-testnet-v2.blockscout.com` |
| **Local (Anvil)** | **`6343`** | **`http://127.0.0.1:8545`** | **—** |

### Non-obvious defaults

- **Tx submission**: use `eth_sendRawTransactionSync` (EIP-7966) — synchronous receipt, no polling.
- **Gas**: base fee is stable at **0.001 gwei**, no EIP-1559. Hardcode gas limits. `eth_maxPriorityFeePerGas` returns 0.
- **Storage**: SSTORE 0→non-zero costs **2M gas × multiplier**. Use Solady's `RedBlackTreeLib` instead of mappings. Design for slot reuse.
- **WebSocket**: send `eth_chainId` every 30s keepalive. Use `miniBlocks` subscription (MegaETH) or `newHeads` subscription (Anvil/standard EVM).
- **Intrinsic gas**: 60,000 (21K compute + 39K storage), not Ethereum's 21K.
- **Volatile data cap**: accessing `block.timestamp` etc. caps total compute gas at 20M *retroactively*.
- **Contract size limit**: 512 KB.
- **State growth**: 1,000 slots per tx (last tx in block can exceed).
- **Debug locally**: `mega-evme replay <txhash> --rpc <endpoint>`.
- **Anvil for MegaETH dev**: Run with `--chain-id 6343 --gas-price 1000000 --block-gas-limit 10000000000 --block-time 1` to simulate MegaETH conditions. The ingestion daemon auto-detects Anvil via chain ID and uses `newHeads` subscription (falls back from `miniBlocks`).

### Key contract addresses (mainnet)

| Contract | Address |
|----------|---------|
| Permit2 | `0x000000000022D473030F116dDEE9F6B43aC78BA3` |
| x402ExactPermit2Proxy | `0x402085c248EeA27D92E8b30b2C58ed07f9E20001` |
| x402UptoPermit2Proxy | `0x402039b3d6E6BEC5A02c2C9fd937ac17A6940002` |
| USDm | `0xFAfDdbb3FC7688494971a79cc65DCa3EF82079E7` |
| DrandOracleQuicknet | `0x7a53a6eFA81c426838fcf4824E6e207923969b36` |
| MegaNames | `0x5B424C6CCba77b32b9625a6fd5A30D409d20d997` |
| DelegationManager | `0xdb9B1e94B5b69Df7e401DDbedE43491141047dB3` |
| DrandOracleQuicknet (testnet) | `0x4e1673dcAA38136b5032F27ef93423162aF977Cc` |

All x402/Permit2 contracts are same addresses on both networks (deterministic CREATE2).

### Skill file map — what to read for specific tasks

| When the task is about… | Read first |
|-------------------------|------------|
| Foundry setup, deploy | `foundry-config.md` |
| Wallet create, balance, send | `wallet-operations.md` |
| React/Next.js + WebSocket UX | `frontend-patterns.md` |
| Privy headless signing | `privy-integration.md` |
| x402 Permit2 payments | `x402-payments.md` |
| Meridian (managed) payments | `meridian.md` |
| RPC methods reference | `rpc-methods.md` |
| Contract design, system contracts | `smart-contracts.md` |
| Storage optimization | `storage-optimization.md` |
| Gas model, estimation | `gas-model.md` |
| Foundry testing, general debug | `testing.md` |
| mega-evme local replay | `mega-evme.md` |
| Security checklist | `security.md` |
| ERC-7710 delegations | `erc7710-delegations.md` |
| MetaMask Smart Accounts | `smart-accounts.md` |
| Warren on-chain websites | `warren.md` |
| MegaNames (.mega) | `meganames.md` |
| drand VRF randomness | `vrf-drand.md` |

### Understanding spec versioning

MegaETH behavior is spec-versioned through `REX5`. Unstable spec changes (e.g., new system-contract behavior) may not be live on testnet/mainnet yet. **Do not assume unstable spec behavior is active unless the user confirms it.**

## Developer commands

```bash
# Run anything
uv run main.py
uv run -m pytest
uv run -m pytest tests/ -v -k "some_test"

# Sync dependencies
uv sync

# Add a dependency
uv add some-package

# MegaETH forge commands (after Foundry setup per foundry-config.md)
forge script Deploy.s.sol --gas-limit 5000000 --skip-simulation
```

## Agent ground rules

1. **Always use `uv run`** instead of bare `python` for executing scripts and tests.
2. For any MegaETH task, **load the `megaeth-developer` skill** and read the relevant file from the skill map above.
3. The Strategy/Reflex Split architecture is the **target design** — implementation code does not exist yet. Build new agent components following this pattern.
4. LangGraph agents must never call `web3.py`. Transaction dispatch goes through the Redis queue → async worker pipeline.
5. Safety constraints (slippage ≤1%, gas fee thresholds) are hard requirements, not suggestions.
