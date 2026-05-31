# Phase 1 — Event-Driven Orchestration System

## TL;DR
> **Quick Summary**: Build the ingestion-to-indexing backbone: WebSocket listener → Redis Stream buffer → PostgreSQL state tracker + Qdrant vector indexer. Four decoupled components, zero LangGraph/transaction code. Foundation for Phase 2 agents.
> **Deliverables**: Ingestion daemon, Redis Stream buffer with consumer groups, PostgreSQL state tracker worker, Qdrant vector indexer worker, FastAPI health/metrics API, Docker Compose infra, full integration test suite
> **Estimated Effort**: Large
> **Parallel Execution**: YES — 4 waves
> **Critical Path**: Wave 1 (schemas + config) → Wave 2 (ingestion + Redis) → Wave 3 (Postgres + Qdrant workers) → Wave 4 (API, tests, polish)

## Context
### Original Request
Build Phase 1 of a decoupled, event-driven orchestration middleware for MegaETH. Four components: ingestion daemon, Redis Stream buffer, PostgreSQL state tracker, Qdrant vector indexer. Strategy/Reflex Split enforced — no LangGraph agents, no transaction dispatch, no web3.py in this phase.

### Interview Summary
- **Test strategy**: Schemas-first (Pydantic contracts) → implement → integration test
- **Package structure**: `app/` with subpackages (`ingestion/`, `streams/`, `state/`, `vector/`)
- **API surface**: FastAPI `/health` + `/metrics` endpoints
- **Embedding**: `cohere-embed:latest` Docker image (self-hosted, port 8000), `embed-english-v3.0` at 1024 dims
- **Stream naming**: `megaeth:raw:miniBlocks`, consumer groups `megaeth:workers:postgres` + `megaeth:workers:qdrant`
- **Docker images**: postgres:16-alpine, redis:7.2-alpine, qdrant/qdrant:latest, cohere-embed:latest (all locally available)

### Metis Review
**P0 findings resolved in this plan:**
1. **ARQ does not support Redis Streams** → Vector indexer uses raw `XREADGROUP` asyncio loop, not ARQ. ARQ reserved for Phase 2 tx dispatch.
2. **"Text summary" undefined** → Locked to structured template: `f"Block #{n}: {tx_count} txns, {gas_used} gas, timestamp {ts}"`. No transaction-level decoding.
3. **cohere-embed image** → Verified locally running on port 8000 via uvicorn. Self-hosted embed service.
4. **No keepalive in ingestion spec** → Added: 30s `eth_chainId` keepalive (MegaETH WS drops idle connections).
5. **What is being "state tracked"?** → Defined: block metadata only (`payload_id`, `block_number`, `index`, `timestamp`, `gas_used`, `raw_jsonb`). No per-address state in Phase 1.

**P1 findings incorporated:**
- MAXLEN 500,000 acknowledged as buffer with ~83 min retention at peak (100 blocks/sec)
- Mini-blocks are ephemeral — missed data on WS outage is gone forever. WARNING logged with gap estimate.
- Dead-letter stream: `megaeth:raw:miniBlocks:dead` for poison pills
- Consumer claim timeout: 30s for XAUTOCLAIM recovery
- LangChain/LangGraph imports banned from Phase 1 (lint rule enforced)
- Graceful shutdown: drain + XACK + close on SIGTERM/SIGINT

### Momus Review (High-Accuracy)
**6 BLOCKERs resolved — schema fixed to match RPC spec, stream schema unified, entrypoints defined, metrics fixed, test injection specified, async cache fixed.**
See end of each task for `[Momus Fix]` annotations.

## Work Objectives
### Core Objective
Build a completely decoupled, event-driven ingestion pipeline that transforms raw MegaETH mini-block WebSocket streams into structured PostgreSQL state records and searchable Qdrant vector embeddings — without any blocking operations, transaction dispatch, or agent reasoning logic.

### Concrete Deliverables
1. **Ingestion daemon** — WebSocket client connecting to MegaETH, XADD'ing miniBlock payloads to Redis Stream with keepalive and exponential backoff
2. **Redis Stream buffer** — Consumer group infrastructure, MAXLEN ~500K, approximate trimming, dead-letter stream
3. **PostgreSQL state tracker** — Consumer worker performing upserts via SQLModel/asyncpg
4. **Qdrant vector indexer** — Consumer worker building text summaries, calling cohere-embed, upserting to Qdrant
5. **FastAPI health/metrics API** — `/health/ready`, `/health/live`, `/metrics`
6. **Docker Compose** — All services with health checks and dependency ordering
7. **Integration tests** — End-to-end synthetic miniBlock injection and verification

### Definition of Done
- [ ] Ingestion daemon connects to MegaETH WebSocket, sends keepalive, XADDs raw payloads without parsing; filters non-subscription messages
- [ ] Redis Stream created with consumer groups, MAXLEN enforced, XTRIM on existing streams
- [ ] PostgreSQL worker upserts miniBlock records with zero duplicates per payload_id (idempotent)
- [ ] Qdrant worker produces exactly one 1024-dim vector per miniBlock (idempotent by payload_id)
- [ ] 100 synthetic miniBlocks flow end-to-end: XADD → Redis Stream → PostgreSQL + Qdrant
- [ ] Health endpoints return correct status codes; metrics expose all counters from `app/core/metrics.py`
- [ ] Chaos tests: Postgres restart recovery, Qdrant restart recovery, Redis restart graceful degradation
- [ ] 24-hour soak test: zero unhandled exceptions, memory stable
- [ ] Docker Compose: `docker compose up` brings all services healthy; supervisor entrypoint starts all 4 async tasks

### Must Have
- Ingestion daemon: asyncio event loop, `websockets` library, 30s keepalive, exponential backoff reconnect, structural Pydantic validation, filters non-subscription WS messages
- Redis Streams: XADD with canonical field name constants from T1; XREADGROUP from consumers; XACK after processing; XAUTOCLAIM for recovery; XTRIM on existing streams
- PostgreSQL: SQLModel models imported from T1 schemas (not redefined); asyncpg driver; sync engine for `create_all` + async engine for operations; ON CONFLICT DO UPDATE upserts; connection pooling
- Qdrant: cohere-embed for 1024-dim embeddings; manual dict-based embedding cache (NOT lru_cache on async); deterministic text summaries; cosine similarity
- FastAPI: single-process supervisor entrypoint that runs all 4 async tasks via asyncio.gather; `/health/ready`, `/health/live`, `/metrics` (Prometheus-compatible counters from shared `app/core/metrics.py`)
- Structured logging via `structlog` with JSONFormatter(serializer=json.dumps); correlation IDs per miniBlock
- Graceful shutdown: asyncio.Event on SIGTERM/SIGINT → drain + XACK + close connections

### Must NOT Have (Guardrails)
- ❌ No ARQ for stream consumers (raw XREADGROUP loop only)
- ❌ No `langchain`, `langchain_openai`, or `langgraph` imports (Phase 2 only)
- ❌ No `web3.py` direct interaction (tx dispatch is Phase 2)
- ❌ No transaction-level decoding of blockchain events
- ❌ No per-address balance tracking (block metadata only)
- ❌ No additional Redis Stream consumer groups beyond Postgres + Qdrant
- ❌ No Grafana dashboards, Prometheus exporters, alert rules
- ❌ No Kafka or alternative message broker
- ❌ No network ports exposed for internal services (Postgres, Redis, Qdrant, cohere-embed are Docker-internal)
- ❌ Text summaries must stay under 500 chars; no full calldata decoding

## Verification Strategy
**Test approach**: Schemas-first (Pydantic contracts), then implementation, then integration tests.
- Unit tests: Pydantic model validation, text summary builder, keepalive timer, backoff calculator (using `fakeredis` where needed)
- Integration tests: Docker Compose services started, synthetic miniBlocks injected via a test WebSocket endpoint, full pipeline verification (using `pytest-asyncio`)
- All verification is agent-executed: the executing agent runs the tests and reports pass/fail. Zero manual verification.
- QA scenarios embedded in each task below.

## Execution Strategy
### Parallel Execution Waves

| Wave | Tasks | Can Run In Parallel | Depends On |
|------|-------|---------------------|------------|
| 1 — Foundation | T1, T2, T3, T4 | YES — all 4 | Nothing |
| 2 — Core Pipeline | T5, T6 | YES — after T1 | T1 (schemas) |
| 3 — Consumers | T7, T8 | YES — both | T5, T6 (pipeline up) |
| 4 — Polish & Verify | T9, T10, T11 | T9 + T10 in parallel; T11 after | T7, T8 (consumers working) |

### Dependency Matrix

```
T1 (schemas) ──┬── T5 (ingestion) ──┬── T7 (postgres worker) ──┬── T9 (health/metrics)
T2 (config)    │   T6 (redis infra)  │   T8 (qdrant worker)     │   T10 (integration tests)
T3 (docker)    │                     │                           │
T4 (logging)   │                     │                           │   T11 (soak test)
               └─────────────────────┴───────────────────────────┴── (after T9+T10)
```

### Agent Dispatch Summary
- Wave 1: 4 agents in parallel (all independent)
- Wave 2: 2 agents in parallel (both depend on T1 schemas, independent of each other)
- Wave 3: 2 agents in parallel (both depend on Wave 2)
- Wave 4: T9 + T10 in parallel, T11 serial after

## TODOs

### Wave 1 — Foundation

- [ ] T1. Define Pydantic Schemas (Contract Layer)
  [Momus Fix #1] **What to do**: Create `app/schemas/` with all message contracts. Files: `app/schemas/__init__.py`, `app/schemas/mini_block.py`, `app/schemas/streams.py`, `app/schemas/state.py`, `app/schemas/vector.py`.
  - `MiniBlockPayload`: raw WebSocket message schema **exactly matching the RPC spec** (see `rpc-methods.md` lines 84-98). Fields: `payload_id: str`, `block_number: int`, `index: int`, `tx_offset: int`, `log_offset: int`, `gas_offset: int`, `timestamp: int` (ms epoch), `gas_used: int`, `transactions: list[str]` (raw tx hashes), `receipts: list[str]` (raw receipt hashes). `tx_count` is **derived**: `len(transactions)`, not a separate field. `block_hash` is NOT in the RPC spec — do not add it.
  - `StreamEntry`: Redis Stream entry wrapper — `block_number: int`, `raw_payload: str` (JSON string), `ingested_at: float` (time.time()). Note: `stream_id` is NOT stored in this model — it comes from Redis XADD return value.
  - `MiniBlockRecord`: PostgreSQL table model (SQLModel, table=True) — `payload_id: str` (unique constraint, for idempotency checks), `block_number: int` (PK — because mini-blocks from same block share block_number, use composite with `index` if needed), `index: int`, `timestamp: int`, `tx_count: int`, `gas_used: int`, `raw_jsonb: dict` (JSONB column), `ingested_at: datetime`. No `block_hash` field — it doesn't exist in RPC data. Store `payload_id` from the wire.
  - `VectorPayload`: Qdrant point descriptor — `payload_id: str`, `block_number: int`, `summary: str`, `embedding: list[float]` (1024-dim), `metadata: dict`. The `payload_id` here is the RPC `payload_id` (hex string), not a generated UUID.
  - `HealthStatus`: FastAPI response — `status: Literal["healthy", "degraded", "unhealthy"]`, `checks: dict[str, str]`, `uptime_seconds: float`
  - Also define Redis Stream field name constants in this module: `STREAM_FIELD_BLOCK_NUMBER = "block_number"`, `STREAM_FIELD_RAW_PAYLOAD = "raw_payload"`, `STREAM_FIELD_INGESTED_AT = "ingested_at"` — these are the canonical keys for the stream. All XADD/XREAD operations must use these constants.
  **Must NOT do**: Import langchain, langgraph, web3, or any Phase 2 dependency. No database or Redis code in schemas.
  **Recommended Agent Profile**: Python type-safety specialist — strong Pydantic v2 skills, SQLModel experience, Literal types
  **Parallelization**: Wave 1, blocks nothing, blocked by nothing
  **References**: `pyproject.toml` (check pydantic-settings version), `AGENTS.md` (MegaETH chain config for types), `.opencode/skills/megaeth-developer/gas-model.md` (block metadata fields)
  **Acceptance Criteria**:
  1. `uv run python -c 'from app.schemas.mini_block import MiniBlockPayload; print("OK")'` succeeds
  2. `uv run python -c 'from app.schemas.state import MiniBlockRecord; print(MiniBlockRecord.model_json_schema())'` outputs valid JSON Schema
  3. `uv run -m pytest tests/unit/test_schemas.py -v` — all schema validation tests pass
  **QA Scenarios**:
  - **Happy path**: Create a valid `MiniBlockPayload` from a JSON dict with all required fields → `model_validate()` succeeds, all fields match. Evidence: `.sisyphus/evidence/task-1-schema-validate.json`
  - **Edge case**: Create `MiniBlockPayload` with missing `block_hash` → `ValidationError` raised with field name in error. Evidence: `.sisyphus/evidence/task-1-missing-field.txt`

- [ ] T2. Configuration System
  **What to do**: Create `app/core/config.py` using `pydantic-settings`. Load from env vars with `.env` file support.
  - `Settings` class with fields: `megaeth_ws_url: str`, `megaeth_chain_id: int`, `redis_url: str`, `postgres_url: str`, `qdrant_url: str`, `cohere_embed_url: str`, `stream_name: str`, `stream_maxlen: int`, `consumer_group_postgres: str`, `consumer_group_qdrant: str`, `keepalive_interval_sec: int`, `reconnect_backoff_base: float`, `log_level: str`
  - Sensible defaults: `megaeth_ws_url="wss://carrot.megaeth.com/ws"` (testnet), `chain_id=6343`, `stream_name="megaeth:raw:miniBlocks"`, `stream_maxlen=500000`, `keepalive_interval_sec=30`
  - Create `.env.example` with all variables documented
  - Create `.env` (gitignored) for local development with `127.0.0.1` addresses for Docker services
  - Verify `.gitignore` includes `.env`; add it if missing
  **Must NOT do**: Hardcode any URLs or secrets. No production credentials.
  **Recommended Agent Profile**: Python developer with pydantic-settings experience, Docker networking knowledge
  **Parallelization**: Wave 1, blocks nothing, blocked by nothing
  **References**: `pyproject.toml` (pydantic-settings version), Docker compose service names for URLs
  **Acceptance Criteria**:
  1. `uv run python -c 'from app.core.config import settings; print(settings.megaeth_ws_url)'` prints a valid URL
  2. `uv run python -c 'from app.core.config import settings; assert settings.stream_maxlen == 500000'` — defaults correct
  3. `.env.example` contains all variables with descriptions
  **QA Scenarios**:
  - **Happy path**: Set `MEGAETH_WS_URL=wss://mainnet.megaeth.com/ws` in env → `settings.megaeth_ws_url` reflects it. Evidence: `.sisyphus/evidence/task-2-env-override.txt`
  - **Edge case**: Missing `.env` file → all defaults load without error. Evidence: `.sisyphus/evidence/task-2-no-env.txt`

- [ ] T3. Docker Compose Infrastructure
  [Momus Fix #7, #21, #22] **What to do**: Create `docker-compose.yml` and `docker-compose.override.yml` for test port exposure. Files: `docker-compose.yml`, `docker-compose.override.yml` (gitignored, for local dev/testing).
  - Services: `postgres` (postgres:16-alpine), `redis` (redis:7.2-alpine), `qdrant` (qdrant/qdrant:latest), `cohere-embed` (cohere-embed:latest), `app` (build from Dockerfile — placeholder in Wave 1, image reference in Wave 4)
  - Health checks: Postgres (`pg_isready`), Redis (`redis-cli ping`), Qdrant (HTTP `/health`), cohere-embed (HTTP `/health`)
  - Dependency ordering: `app` depends_on all services with `condition: service_healthy`. In Wave 1, use `profiles: ["app"]` on the app service so it doesn't block `docker compose up` before the Dockerfile exists.
  - Redis config: command override `--maxmemory 2gb --maxmemory-policy noeviction --appendonly no`
  - Postgres config: env vars `POSTGRES_USER=megaeth`, `POSTGRES_PASSWORD=megaeth`, `POSTGRES_DB=megaeth`. **Mount named volume**: `postgres_data:/var/lib/postgresql/data` for persistence.
  - Qdrant: mount named volume `qdrant_data:/qdrant/storage` for persistence
  - cohere-embed: port 8000 (internal only, not exposed to host)
  - App: port 8080 exposed to host for health/metrics. Uses `DOCKER_ENV=true` env var.
  - Network: all services on `megaeth-net` bridge
  - **`docker-compose.override.yml`** (for tests): exposes internal ports to host — Postgres: `5432:5432`, Redis: `6379:6379`, Qdrant: `6333:6333`, cohere-embed: `8000:8000`. This file is gitignored and only used locally for integration tests.
  - Named volumes: `postgres_data`, `qdrant_data`
  **Must NOT do**: Expose Postgres/Redis/Qdrant/cohere-embed ports in production compose. No hardcoded credentials (use env vars or `.env`).
  **Recommended Agent Profile**: DevOps/infrastructure specialist — Docker Compose, health checks, volume management
  **Parallelization**: Wave 1, blocks nothing, blocked by nothing
  **References**: Docker images already verified locally (see interview summary), `AGENTS.md` for chain context
  **Acceptance Criteria**:
  1. `docker compose up -d postgres redis` → both services healthy within 30s
  2. `docker compose up -d qdrant cohere-embed` → both services healthy within 30s
  3. `docker compose ps` shows all 4 infra services as healthy
  4. `docker compose down -v` cleans up volumes
  **QA Scenarios**:
  - **Happy path**: Full `docker compose up -d` (all infra services) → all healthy within 60s, `docker compose logs` shows no errors. Evidence: `.sisyphus/evidence/task-3-compose-up.txt`
  - **Edge case**: Start with Postgres only, then bring up remaining → health checks pass independently, app service waits for all. Evidence: `.sisyphus/evidence/task-3-incremental.txt`
  - **Failure mode**: Kill Redis container → `docker compose ps` shows redis as unhealthy, Qdrant and cohere-embed remain healthy (independent). Evidence: `.sisyphus/evidence/task-3-redis-down.txt`

- [ ] T4. Structured Logging Setup
  [Momus Fix #30, #32] **What to do**: Create `app/core/logging.py` configuring `structlog` for JSON-formatted output.
  - `setup_logging()` function: configures structlog with processors (filter_by_level, add_log_level, TimeStamper(fmt="iso"), JSONRenderer(serializer=json.dumps))
  - Log level controlled via `settings.log_level` (DEBUG/INFO/WARNING/ERROR)
  - Correlation ID support: `structlog.contextvars.bind_contextvars(correlation_id=...)` pattern. Correlation IDs generated per miniBlock in the ingestion daemon (uuid4) and included in stream entry.
  - Helper: `get_logger(name: str)` returning a bound logger
  **Must NOT do**: Log sensitive data (private keys, full payloads). No print() statements.
  **Recommended Agent Profile**: Python developer with structlog experience
  **Parallelization**: Wave 1, blocks nothing, blocked by nothing
  **References**: `pyproject.toml` (structlog version), Python development standards (structured logging section)
  **Acceptance Criteria**:
  1. `uv run python -c 'from app.core.logging import setup_logging, get_logger; setup_logging(); logger = get_logger("test"); logger.info("hello")'` outputs valid JSON to stdout
  2. JSON output includes: `event`, `level`, `logger`, `timestamp`
  **QA Scenarios**:
  - **Happy path**: Log at INFO, WARNING, ERROR levels → each produces valid JSON with correct level field. Evidence: `.sisyphus/evidence/task-4-log-levels.jsonl`
  - **Edge case**: Bind correlation ID → all subsequent log lines include the correlation_id. Evidence: `.sisyphus/evidence/task-4-correlation.jsonl`

### Wave 2 — Core Pipeline

- [ ] T5. Real-Time Ingestion Daemon
  [Momus Fix #2, #3] **What to do**: Create `app/ingestion/` package. Files: `app/ingestion/__init__.py`, `app/ingestion/daemon.py`, `app/ingestion/backoff.py`.
  - `MegaETHIngestionDaemon` class: async context manager
  - `connect()`: establishes WebSocket to `settings.megaeth_ws_url` using `websockets.connect()`
  - `subscribe()`: sends `{"jsonrpc":"2.0","method":"eth_subscribe","params":["miniBlocks"],"id":1}` after connection
  - `_keepalive_loop()`: sends `{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":...}` every `settings.keepalive_interval_sec` (30s). **Must filter keepalive responses** — they arrive as `{"jsonrpc":"2.0","id":...,"result":"0x..."}` and must NOT be routed to the message handler. Only `method == "eth_subscription"` messages are miniBlocks.
  - `_message_handler()`: on WS message, checks `parsed.get("method") == "eth_subscription"`, extracts `params.result` as raw JSON string, XADDs to Redis Stream using the canonical field names: `redis.xadd(stream_name, {STREAM_FIELD_BLOCK_NUMBER: str(n), STREAM_FIELD_RAW_PAYLOAD: json_str, STREAM_FIELD_INGESTED_AT: str(time.time())})`. Note: Redis Stream values are always `str`/`bytes` — convert numerics to strings.
  - **Recommended**: add lightweight structural validation — `MiniBlockPayload.model_validate_json(raw_json)` — catch `ValidationError`, log WARNING, skip (do NOT XADD invalid data). This prevents bad data from entering the stream.
  - **Zero business logic**: no parsing of transactions, no decoding, no validation beyond structural Pydantic check
  - `_reconnect()`: on disconnect, waits with exponential backoff (1s → 2s → 4s → 8s → 16s cap) before reconnecting
  - `run()`: main async loop, starts keepalive + message handler, catches exceptions, logs, reconnects
  - **Entrypoint**: `app/ingestion/daemon.py` must include `async def main():` that creates the daemon and calls `await daemon.run()`. This is importable by the supervisor (T9).
  - Metrics: increment `blocks_ingested` counter, `ws_reconnects` counter (counters live in `app/core/metrics.py`)
  - Graceful shutdown: on SIGTERM/SIGINT, set `asyncio.Event`, close WS, drain pending XADD, log final metrics
  - `app/ingestion/backoff.py`: `ExponentialBackoff` helper class — `base: float`, `cap: float`. On first disconnect after a stable connection, reset to base. On repeated rapid disconnects, continue exponential progression.
  **Must NOT do**: Parse transaction data, decode logs, read block timestamps, call any RPC method other than `eth_subscribe` and `eth_chainId`. No database access. No Qdrant access.
  **Recommended Agent Profile**: Async Python specialist — websockets library, asyncio event loops, context managers
  **Parallelization**: Wave 2, blocks T7+T8, blocked by T1 (schemas, for miniBlock validation)
  **References**: `.opencode/skills/megaeth-developer/frontend-patterns.md` (WebSocket keepalive pattern, lines 1-68), `.opencode/skills/megaeth-developer/rpc-methods.md` (miniBlock subscription format, lines 73-80), T1 schemas for `MiniBlockPayload`
  **Acceptance Criteria**:
  1. Daemon connects to MegaETH WS and receives `eth_subscription` messages within 10s of startup
  2. Keepalive `eth_chainId` sent every 30s (±2s tolerance)
  3. Each miniBlock's XADD call to Redis completes in ≤50ms (measure: `time.monotonic()` before and after `redis.xadd()` in `_message_handler()` — NOT comparing chain timestamp to daemon time, which measures network latency)
  4. On intentional WS disconnect, reconnects with backoff: 1s → 2s → 4s → 8s → 16s (cap)
  5. `XLEN megaeth:raw:miniBlocks` grows by 1 per miniBlock received
  **QA Scenarios**:
  - **Happy path**: Start daemon connected to testnet → within 30s, `redis-cli XLEN megaeth:raw:miniBlocks` > 0. Evidence: `.sisyphus/evidence/task-5-ingestion-happy.txt`
  - **Keepalive**: Monitor WS traffic for 2 minutes → `eth_chainId` appears 4 times (±1). Evidence: `.sisyphus/evidence/task-5-keepalive.txt`
  - **Reconnect**: Kill the WS connection externally → daemon logs "disconnected, reconnecting in 1s", reconnects successfully. Evidence: `.sisyphus/evidence/task-5-reconnect.txt`
  - **Poison pill**: Inject malformed JSON via test helper → daemon logs WARNING, does NOT crash, continues processing. Evidence: `.sisyphus/evidence/task-5-poison-pill.txt`

- [ ] T6. Redis Stream Buffer Infrastructure
  [Momus Fix #2, #12] **What to do**: Create `app/streams/` package. Files: `app/streams/__init__.py`, `app/streams/buffer.py`, `app/streams/consumer.py`.
  - `StreamBuffer` class: manages Redis Stream lifecycle
    - `initialize()`: checks if stream exists; if not, creates it via `XADD stream * init done` then immediately XDELs the init entry. Then runs `XTRIM stream MAXLEN ~500000` to enforce the limit (handles pre-existing streams without MAXLEN). If stream already exists, just runs XTRIM.
    - `create_consumer_group()`: `XGROUP CREATE stream group $ MKSTREAM` (idempotent — handles "BUSYGROUP Consumer Group name already exists" error)
    - `add(block_number: int, raw_payload: str)` helper: `XADD stream * block_number {n} raw_payload {json} ingested_at {ts}` using the canonical field name constants from T1 schemas. Used by the ingestion daemon (T5) and test injection (T10).
  - `StreamConsumer` base class: async generator consuming from group
    - `consume(group: str, consumer: str, block_ms: int = 1000)`: `XREADGROUP GROUP group consumer BLOCK block_ms STREAMS stream >` → yields `(stream_id, dict[str, bytes])` tuples. **Shorter block (1000ms vs 5000ms) for faster shutdown.**
    - `ack(stream_id: str)`: `XACK stream group stream_id`
    - `claim_pending(min_idle_ms: int = 30000)`: `XAUTOCLAIM stream group consumer min_idle_ms 0-0` → reclaims orphaned messages
    - `dead_letter(stream_id: str, data: dict)`: `XADD megaeth:raw:miniBlocks:dead * ...` with original field names → moves poison pill to dead stream
    - `parse_entry(data: dict[str, bytes]) -> StreamEntry`: helper that decodes bytes → typed fields using the T1 schema. All consumers (T7, T8) use this method for consistent decoding.
  - Dead letter stream: `megaeth:raw:miniBlocks:dead` — created automatically on first poison pill
  - Metrics: stream length, consumer group lag (total pending via `XPENDING`), dead letter count
  **Must NOT do**: Hardcode consumer names. No synchronous Redis operations (all async via `redis.asyncio`).
  **Recommended Agent Profile**: Redis specialist — Redis Streams, consumer groups, XREADGROUP/XACK/XAUTOCLAIM semantics
  **Parallelization**: Wave 2, blocks T7+T8, blocked by T1 (schemas, for StreamEntry type)
  **References**: `redis` library docs for asyncio Stream commands, T1 schemas for `StreamEntry`, `settings` for stream_name and maxlen
  **Acceptance Criteria**:
  1. `StreamBuffer.initialize()` creates stream, `XLEN` = 0
  2. `StreamBuffer.add(payload)` returns a valid Redis Stream ID (e.g., `1234567890123-0`)
  3. `StreamBuffer.create_consumer_group("megaeth:workers:postgres")` succeeds; second call handles BUSYGROUP gracefully
  4. `StreamConsumer.consume()` yields messages in order; `ack()` removes from PEL
  5. After 1M+ XADDs, `XLEN` ≈ 500,000 (approximate trimming active)
  6. `XAUTOCLAIM` recovers messages idle >30s from a simulated crashed consumer
  **QA Scenarios**:
  - **Happy path**: XADD 10 entries → XREADGROUP reads all 10 in order → XACK all → PEL empty. Evidence: `.sisyphus/evidence/task-6-consumer-group.txt`
  - **MAXLEN**: XADD 600,000 entries → `XLEN` ≤ 500,000 (approximate). Evidence: `.sisyphus/evidence/task-6-maxlen.txt`
  - **Dead letter**: XADD malformed entry → consumer routes to dead stream → `XLEN megaeth:raw:miniBlocks:dead` = 1. Evidence: `.sisyphus/evidence/task-6-dead-letter.txt`
  - **XAUTOCLAIM**: Consumer A reads entry, crashes (no XACK) → after 30s, Consumer B calls XAUTOCLAIM → receives orphaned entry. Evidence: `.sisyphus/evidence/task-6-autoclaim.txt`

### Wave 3 — Consumers

- [ ] T7. Relational State Tracker Worker
  [Momus Fix #1, #8, #25] **What to do**: Create `app/state/` package. Files: `app/state/__init__.py`, `app/state/worker.py`, `app/state/repository.py`, `app/state/database.py`.
  - **Do NOT create `app/state/models.py`** — import `MiniBlockRecord` directly from `app.schemas.state` (T1 defines the canonical model).
  - `app/state/database.py`: creates **both** engines:
    - Sync engine for table creation: `create_engine(settings.postgres_url.replace("+asyncpg", ""))` — used once at startup for `SQLModel.metadata.create_all(sync_engine)`
    - Async engine for operations: `create_async_engine(settings.postgres_url, pool_size=5, max_overflow=10)` — used by `async_sessionmaker`
  - `app/state/repository.py`: `StateRepository` class
    - `upsert(record: MiniBlockRecord)`: executes `INSERT INTO mini_block_record (payload_id, block_number, index, timestamp, tx_count, gas_used, raw_jsonb, ingested_at) VALUES (...) ON CONFLICT (block_number) DO UPDATE SET ...` — updates all fields. Note: PK is `block_number`; `payload_id` has a unique index for idempotency checks.
    - `get_latest_block() -> int | None`: `SELECT MAX(block_number) FROM mini_block_record`
  - `app/state/worker.py`: `StateTrackerWorker` class — includes `async def main():` entrypoint importable by T9 supervisor
    - `run()`: async loop — `consume()` from Redis Stream consumer group `megaeth:workers:postgres` → `parse_entry()` (from T6) → convert to `MiniBlockRecord` (derive `tx_count = len(payload.transactions)`) → `upsert()` → `ack()`
    - Poison pill handling: if parsing/upsert fails 3 times → `dead_letter()` and `ack()` (skip)
    - Startup: creates consumer group if not exists, calls `SQLModel.metadata.create_all(sync_engine)` (via sync engine), logs readiness
    - Graceful shutdown: set `asyncio.Event`, drain current message → `ack()` → close DB pool
    - Metrics: `blocks_processed` counter (in `app/core/metrics.py`), `blocks_failed` counter
  **Must NOT do**: Track per-address balances. Decode transaction calldata. Query Qdrant or cohere-embed.
  **Recommended Agent Profile**: SQLModel/PostgreSQL specialist — asyncpg, connection pooling, upsert patterns, SQLModel table definitions
  **Parallelization**: Wave 3, blocks nothing, blocked by T5 (ingestion producing data) + T6 (Redis Stream buffer ready)
  **References**: T1 schemas (MiniBlockRecord), T6 (StreamConsumer base class), `pyproject.toml` (sqlmodel, asyncpg versions), Python development standards (repository pattern)
  **Acceptance Criteria**:
  1. Worker starts, creates consumer group, initializes DB tables, begins consuming
  2. Each miniBlock from stream → exactly one row in `mini_block_record` table (no duplicates, no gaps for processed blocks)
  3. `ON CONFLICT DO UPDATE` correctly overwrites when same block_number arrives (verify: values match latest stream entry)
  4. Worker survives PostgreSQL restart: reconnects automatically, resumes from last acknowledged Stream ID
  5. Poison pill (malformed JSON in stream) → logged, moved to dead stream, worker continues
  6. Processes ≥10 miniBlocks/sec sustained (baseline; benchmark to find ceiling)
  **QA Scenarios**:
  - **Happy path**: Inject 50 synthetic miniBlocks into Redis Stream → worker processes all 50 → `SELECT COUNT(*) FROM mini_block_record` = 50. Evidence: `.sisyphus/evidence/task-7-upsert-50.txt`
  - **Idempotency**: XADD same block_number twice → worker upserts both → `COUNT(*)` = 1 for that block_number, record matches second entry. Evidence: `.sisyphus/evidence/task-7-idempotent.txt`
  - **PostgreSQL restart**: Kill Postgres for 15s → worker logs connection errors → Postgres recovers → worker reconnects → processes next message from where it left off (via XREADGROUP with `>`). Evidence: `.sisyphus/evidence/task-7-pg-recovery.txt`
  - **Poison pill**: XADD entry with invalid JSON → worker retries 3x → moves to dead stream → XACKs → continues. Evidence: `.sisyphus/evidence/task-7-poison-pill.txt`

- [ ] T8. Cognitive Analytics Vector Indexer
  [Momus Fix #3, #6] **What to do**: Create `app/vector/` package. Files: `app/vector/__init__.py`, `app/vector/worker.py`, `app/vector/embedder.py`, `app/vector/summarizer.py`, `app/vector/qdrant_client.py`.
  - `app/vector/summarizer.py`: `build_summary(payload: MiniBlockPayload) -> str`
    - Template: `f"Block #{payload.block_number}: {len(payload.transactions)} transactions, {payload.gas_used} gas used, timestamp {payload.timestamp}"`
    - Max 500 characters. No transaction-level decoding. For empty blocks: `f"Block #{payload.block_number}: empty"`
  - `app/vector/embedder.py`: `CohereEmbedder` class
    - `embed(text: str) -> list[float]`: POST to `{settings.cohere_embed_url}/v1/embeddings` with `model="embed-english-v3.0"`, `texts=[text]`, `input_type="search_document"`. **Before implementing, verify the actual endpoint by querying the running cohere-embed container** (`curl http://localhost:8000/docs` or test the POST). The endpoint path may differ from the Cohere API.
    - Validates response: 1024 dimensions, all floats
    - **Cache: manual `dict`-based cache**, NOT `functools.lru_cache`. `lru_cache` on async functions caches the coroutine, not the result. Use: `self._cache: dict[str, list[float]] = {}` with `if summary in self._cache: return self._cache[summary]`. LRU eviction optional for Phase 1 (just use `dict` and accept unbounded growth — max 10K entries is ~80MB).
    - `health_check() -> bool`: calls embed endpoint with small test string "health check"
  - `app/vector/qdrant_client.py`: `QdrantManager` class
    - `initialize()`: creates collection `megaeth_blocks` with `vectors: {size: 1024, distance: "Cosine"}` if not exists (`recreate=False` to preserve data on restart)
    - `upsert(payload: VectorPayload)`: `client.upsert(collection_name, points=[{id: payload.payload_id, vector: payload.embedding, payload: {"block_number": payload.block_number, "summary": payload.summary, "metadata": payload.metadata}}])`. The `id` is the RPC `payload_id` (hex string), ensuring idempotent upserts.
    - `search(vector: list[float], limit: int = 10)`: cosine similarity search in collection
  - `app/vector/worker.py`: `VectorIndexerWorker` class — includes `async def main():` entrypoint importable by T9 supervisor
    - `run()`: async loop — `consume()` from Redis Stream consumer group `megaeth:workers:qdrant` → `parse_entry()` (from T6) → build `MiniBlockPayload` → `build_summary()` → `embed()` → `upsert()` → `ack()`
    - Degraded mode: if cohere-embed unavailable → log WARNING → skip embedding → XACK anyway (pipeline continues without that block's vector). Do NOT crash.
    - If Qdrant unavailable → log ERROR, retry with backoff, don't XACK until successful (message stays in PEL).
    - Poison pill handling: 3 failed processing attempts → dead letter + XACK
    - Startup: creates collection, verifies cohere-embed health
    - Graceful shutdown: set `asyncio.Event`, drain → embed → upsert → XACK → close connections
    - Metrics: counters in `app/core/metrics.py` — `vectors_upserted`, `embeddings_cached` (hits), `embeddings_computed` (misses), `degraded_events`
  **Must NOT do**: Use ARQ. Import langchain/langgraph. Decode transaction calldata in summaries. Create Qdrant collection with `recreate=True` (data loss).
  **Recommended Agent Profile**: Vector DB / ML specialist — Qdrant client, embedding APIs, cosine similarity, caching strategies
  **Parallelization**: Wave 3, blocks nothing, blocked by T5 (ingestion producing data) + T6 (Redis Stream buffer ready)
  **References**: T1 schemas (VectorPayload, MiniBlockPayload), cohere-embed Docker image (port 8000, tested locally), qdrant-client docs for collection management and upsert
  **Acceptance Criteria**:
  1. Worker starts, creates Qdrant collection (1024-dim, Cosine), verifies cohere-embed health, begins consuming
  2. Each miniBlock → exactly one vector point in Qdrant (verify: `collection.points_count` matches blocks processed)
  3. Embedding is deterministic: same summary → same vector (verify: embed twice, cosine similarity ≥ 0.9999)
  4. Cosine search for known block's vector returns itself as top result (score ≥ 0.99)
  5. LRU cache: duplicate summaries (same block replayed) → cache hit, no API call (verify via `embeddings_cached` metric)
  6. Degraded mode: stop cohere-embed container → worker logs WARNING, skips embeddings, XACKs, continues
  7. Qdrant restart: worker retries with backoff, resumes successfully
  **QA Scenarios**:
  - **Happy path**: Inject 25 synthetic miniBlocks → worker processes all 25 → Qdrant collection has 25 points. Evidence: `.sisyphus/evidence/task-8-vector-25.txt`
  - **Determinism**: Inject same miniBlock payload twice → LRU cache hit on second → Qdrant point count = 1 for that block (upsert idempotent). Evidence: `.sisyphus/evidence/task-8-deterministic.txt`
  - **Degraded mode**: Stop cohere-embed container → inject 10 miniBlocks → worker skips embedding for all 10, XACKs all, continues → restart cohere-embed → next block gets embedded normally. Evidence: `.sisyphus/evidence/task-8-degraded.txt`
  - **Qdrant unavailable**: Stop Qdrant → inject 5 miniBlocks → worker retries (backoff), doesn't XACK → restart Qdrant → worker upserts all 5 → XACKs. Evidence: `.sisyphus/evidence/task-8-qdrant-recovery.txt`

### Wave 4 — Polish & Verify

- [ ] T9. FastAPI Health & Metrics API
  [Momus Fix #3, #4, #16, #23] **What to do**: Create `app/api/` and `app/core/metrics.py`. Files: `app/api/__init__.py`, `app/api/main.py`, `app/api/routes/health.py`, `app/api/routes/metrics.py`, `app/api/dependencies.py`, `app/core/metrics.py`.
  - **`app/core/metrics.py`**: shared metrics module for all components. Uses module-level atomic counters (NOT a singleton class — all async tasks in the same process share module globals). `blocks_ingested_total: int`, `ws_reconnects_total: int`, `vectors_upserted_total: int`, `embeddings_cached_total: int`, `embeddings_computed_total: int`, `degraded_events_total: int`, `blocks_processed_total: int`, `blocks_failed_total: int`. All counters use simple `int` with type annotation; increment is `counter += 1`. Thread safety via `asyncio.Lock()` for each counter or just rely on asyncio cooperative multitasking (no true parallelism). Also tracks `last_block_number: int | None`.
  - `app/api/main.py`: FastAPI app factory — `create_app()` returns configured FastAPI instance. **Also defines a `async def main()` supervisor entrypoint** that starts all 4 async tasks concurrently: `await asyncio.gather(ingestion_main(), state_worker_main(), vector_worker_main(), serve_app())` where `serve_app()` runs the uvicorn server. This is the single entrypoint for the Docker container.
  - `app/api/routes/health.py`:
    - `GET /health/live`: always returns `{"status": "ok"}` (process alive). Docker HEALTHCHECK target.
    - `GET /health/ready`: checks PostgreSQL (`SELECT 1` via sync engine), Redis (`PING`), Qdrant (`collection_exists`), cohere-embed (`health_check()`). Returns `{"status": "healthy|degraded|unhealthy", "checks": {...}, "uptime_seconds": ...}`. HTTP 200 if healthy, 503 if unhealthy.
    - Dependencies: use async FastAPI dependencies via `async def get_db_session()` → `AsyncGenerator[AsyncSession, None]` with `yield session; await session.close()` in finally.
  - `app/api/routes/metrics.py`:
    - `GET /metrics`: returns Prometheus-compatible text format with all counters from `app.core.metrics`. Plain text, no external library. Format: `megaeth_blocks_ingested_total {value}`
  - **Docker integration**: `Dockerfile` (multi-stage):
    - Stage 1: `FROM ghcr.io/astral-sh/uv:python3.11-bookworm AS builder` — `COPY pyproject.toml uv.lock ./` → `uv sync --no-dev --frozen`
    - Stage 2: `FROM python:3.11-slim` — copy `.venv` from builder, copy `app/` source, `USER appuser`
    - CMD: `uv run python -m app.api.main` (runs the supervisor entrypoint that starts all 4 tasks)
  - **Docker networking**: In `docker-compose.yml`, the `app` service uses Docker DNS names: `redis://redis:6379/0`, `postgresql+asyncpg://user:pass@postgres:5432/megaeth`, etc. The `.env` file uses `127.0.0.1` for local dev (host). Add `DOCKER_ENV=true` env var to docker-compose so the app can detect Docker mode.
  **Must NOT do**: Import web3, langchain, langgraph. Add authentication. Create Grafana dashboards.
  **Recommended Agent Profile**: FastAPI specialist — APIRouter, dependency injection, health check patterns, Prometheus text format
  **Parallelization**: Wave 4, blocks T11, blocked by T7+T8 (need consumers running to wire health checks)
  **References**: T2 (config for URLs), T4 (logging), `fastapi[standard]` in pyproject.toml, Python development standards (FastAPI architecture, health check patterns)
  **Acceptance Criteria**:
  1. `GET /health/live` returns 200 and `{"status": "ok"}` within 50ms
  2. `GET /health/ready` returns 200 with all checks "healthy" when all services are up
  3. `GET /health/ready` returns 503 when PostgreSQL is down
  4. `GET /metrics` returns valid Prometheus text format with all counters
  5. Docker `HEALTHCHECK` in Dockerfile passes using `/health/live`
  **QA Scenarios**:
  - **Happy path (curl)**: `curl http://localhost:8080/health/ready` → 200, all checks healthy. Evidence: `.sisyphus/evidence/task-9-health-ready.txt`
  - **Degraded (curl)**: Stop Postgres → `curl http://localhost:8080/health/ready` → 503, postgres check "unhealthy". Evidence: `.sisyphus/evidence/task-9-health-degraded.txt`
  - **Metrics (curl)**: `curl http://localhost:8080/metrics` → text output with `megaeth_blocks_ingested_total` counter present. Evidence: `.sisyphus/evidence/task-9-metrics.txt`

- [ ] T10. Integration Test Suite
  [Momus Fix #5, #7, #18, #19] **What to do**: Create `tests/` directory with full test infrastructure. Files: `tests/conftest.py`, `tests/unit/test_schemas.py`, `tests/unit/test_summarizer.py`, `tests/integration/test_pipeline.py`, `tests/integration/test_chaos.py`.
  - **Test data injection mechanism**: Integration tests inject data by **directly XADDing to the Redis Stream** (bypassing WebSocket). Use the same `StreamBuffer.add()` method from T6. This is the simplest, most decoupled approach — tests control exactly what enters the pipeline. **No test WebSocket endpoint needed.**
  - `tests/conftest.py`: pytest fixtures
    - `redis_client`: connects to real Redis from Docker compose. Uses `redis://127.0.0.1:6379/1` (db 1 for testing). Also supports `fakeredis` via `REDIS_TEST_MODE=fake` env var for unit tests.
    - `db_session`: async SQLModel session with test database (separate db or schema from production)
    - `qdrant_client`: connects to Qdrant on `http://127.0.0.1:6333` (test collection `megaeth_blocks_test`)
    - `mini_block_factory`: factory function producing `MiniBlockPayload` with sequential `block_number`s and auto-generated `payload_id`s (hex strings)
    - `test_stream_name`: uses `megaeth:test:miniBlocks` (NOT production stream name). Override via `TEST_STREAM_NAME` env var.
    - `setup_stream`: creates test stream + consumer groups before each integration test, cleans up after
  - `tests/unit/test_schemas.py`: validates all Pydantic models — required fields, type coercion, invalid inputs raise ValidationError. Tests that `MiniBlockPayload` does NOT accept `block_hash` (it's not in the RPC spec).
  - `tests/unit/test_summarizer.py`: validates `build_summary()` for normal blocks, empty blocks, boundary conditions
  - `tests/integration/test_pipeline.py`:
    - `test_e2e_100_blocks`: XADD 100 synthetic miniBlocks to test stream → verify all 100 in PostgreSQL + all 100 in Qdrant
    - `test_consumer_ordering`: verify blocks processed in order (block_number ascending)
    - `test_idempotent_upsert`: XADD same payload_id twice → no duplicates in Postgres or Qdrant
    - `test_dead_letter_routing`: XADD malformed payload → ends up in dead stream
  - `tests/integration/test_chaos.py`: uses `subprocess.run(["docker", "stop", ...])` and `subprocess.run(["docker", "start", ...])` to control containers. Add `SKIP_CHAOS_TESTS=1` env var for environments without Docker access.
    - `test_postgres_restart_recovery`: stop Postgres (15s), restart, verify worker recovers
    - `test_qdrant_restart_recovery`: stop Qdrant (15s), restart, verify worker recovers
    - `test_redis_restart_graceful`: stop Redis (10s), restart, verify daemon reconnects
    - `test_cohere_degraded_mode`: stop cohere-embed, verify vector worker enters degraded mode
  - **Docker connectivity for tests**: Add `docker-compose.override.yml` to T3 that exposes internal ports to host for testing: Postgres:5432, Redis:6379, Qdrant:6333, cohere-embed:8000. Tests on host connect via `127.0.0.1`.
  - `pyproject.toml` `[tool.pytest.ini_options]`: `asyncio_mode = "auto"`, `testpaths = ["tests"]`
  - `pyproject.toml` `[tool.ruff]`: minimal config — `target-version = "py311"`, `line-length = 120`
  - `pyproject.toml` `[tool.mypy]`: `strict = true`, with override for pydantic: `[[tool.mypy.overrides]] module = "pydantic.*" ignore_missing_imports = true`
  **Must NOT do**: Use production network endpoints. Hardcode test assertions to specific block data.
  **Recommended Agent Profile**: Test engineering specialist — pytest, pytest-asyncio, fixtures, factories, chaos testing
  **Parallelization**: Wave 4, blocks T11, blocked by T7+T8 (consumers working)
  **References**: T7 (state worker), T8 (vector worker), `pyproject.toml` (pytest, pytest-asyncio, fakeredis, pytest-cov versions)
  **Acceptance Criteria**:
  1. `uv run -m pytest tests/unit/ -v` — all unit tests pass
  2. `uv run -m pytest tests/integration/test_pipeline.py -v` — all pipeline tests pass with Docker services running
  3. `uv run -m pytest tests/integration/test_chaos.py -v` — all chaos tests pass
  4. `uv run -m pytest --cov=app --cov-report=term` — coverage ≥ 75%
  **QA Scenarios**:
  - **E2E pipeline**: `uv run -m pytest tests/integration/test_pipeline.py::test_e2e_100_blocks -v` → PASS, all 100 blocks verified in both stores. Evidence: `.sisyphus/evidence/task-10-e2e-100.txt`
  - **Chaos**: `uv run -m pytest tests/integration/test_chaos.py -v` → all 4 chaos tests PASS. Evidence: `.sisyphus/evidence/task-10-chaos.txt`

- [ ] T11. Soak Test & Final Polish
  [Momus Fix #27] **What to do**: Endurance validation. Files: `tests/soak/test_soak.py`, `tests/soak/conftest.py`.
  - 24-hour soak test (can be run for shorter durations in CI, e.g., 1 hour minimum; scale to 24h for production readiness)
  - Injects synthetic miniBlocks at 100/sec (peak design load) for the test duration via direct Redis XADD
  - Monitors: consumer lag (must not grow unbounded), memory usage (must stay within 10% of baseline), error rate (must be 0 unhandled exceptions)
  - Logs periodic snapshots: every 5 minutes, log block count, lag, memory
  - Final verification: all injected blocks accounted for in Postgres (exact match); ≥95% of blocks have Qdrant vectors (accounting for cohere-embed degraded windows)
  - Fix any issues found: PEL growth, connection leaks, memory leaks
  - Final cleanup: remove any debug logging, ensure all TODO comments resolved, verify `.env.example` is complete
  **Must NOT do**: Add new features. Change architecture. Introduce Phase 2 dependencies.
  **Recommended Agent Profile**: Generalist with performance testing experience
  **Parallelization**: Wave 4, blocks nothing, blocked by T9 (API running) + T10 (integration tests passing)
  **References**: T5-T10 deliverables, `AGENTS.md` (architecture constraints)
  **Acceptance Criteria**:
  1. 1-hour soak (minimum): inject at 50 miniBlocks/sec → consumer lag ≤ 5s steady state, memory stable
  2. Zero unhandled exceptions in logs
  3. All injected blocks present in PostgreSQL (exact match)
  4. ≥95% of blocks have Qdrant vectors (accounting for cohere-embed degraded windows)
  5. `uv run -m pytest tests/soak/ -v` passes
  **QA Scenarios**:
  - **1-hour soak**: Run test, verify final assertions. Evidence: `.sisyphus/evidence/task-11-soak-summary.txt`
  - **Memory stability**: Plot memory usage over test duration → flat trendline (±10%). Evidence: `.sisyphus/evidence/task-11-memory.csv`

## Final Verification Wave
- [ ] F1. Plan Compliance Audit (oracle): Verify no langchain/langgraph/web3 imports, no ARQ for stream consumers, all guardrails respected
- [ ] F2. Code Quality Review: `uv run -m ruff check app/` — zero errors, `uv run -m mypy app/ --strict` — zero errors
- [ ] F3. Full Integration QA: `docker compose up -d` → `uv run -m pytest tests/ -v` → all pass → `docker compose down -v`
- [ ] F4. Scope Fidelity Check: confirm exactly 4 components delivered, no Phase 2 creep, all Must NOT Have respected

## Commit Strategy
- Commit each wave as a single commit after all tasks in the wave pass verification:
  1. `phase1: foundation — schemas, config, docker, logging`
  2. `phase1: core pipeline — ingestion daemon, redis stream buffer`
  3. `phase1: consumers — postgres state tracker, qdrant vector indexer`
  4. `phase1: polish — health API, integration tests, soak test`

## Success Criteria
1. `docker compose up -d` brings all 5 services healthy within 60s
2. Ingestion daemon connects to MegaETH WS, sends keepalives, XADDs without parsing
3. Redis Stream buffer enforces MAXLEN, consumer groups distribute work
4. PostgreSQL has zero duplicate blocks; Qdrant has exactly one vector per block
5. 100-block E2E test passes: all blocks in both stores
6. Health endpoints respond correctly; metrics counters increment
7. Chaos tests pass: Postgres/Qdrant/Redis/cohere-embed restart recovery
8. 1-hour soak test passes with stable memory and bounded consumer lag
9. Zero Phase 2 dependencies imported; all guardrails respected
