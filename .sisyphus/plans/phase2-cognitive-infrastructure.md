# Phase 2: Cognitive Infrastructure Layer

## TL;DR
> **Quick Summary**: Build the infrastructure layer that bridges LangGraph cognition (future) to MegaETH execution — Pydantic intent schemas, SQLModel asset state tracking with ERC-20 Transfer event extraction, and an async worker pool that signs and broadcasts transactions via Redis Streams.
> **Deliverables**: `app/schemas/intent.py`, `app/state/models.py`, `app/agents/dispatcher.py`, `app/agents/dispatch.py` (ExecutionQueue), `app/state/transfer_parser.py`, `app/state/balance_repository.py`, `app/state/balance_worker.py`, `app/api/dependencies.py`, supervisor integration, ~40 tests
> **Estimated Effort**: Medium
> **Parallel Execution**: YES — 5 waves, 12 tasks
> **Critical Path**: T1 → T4 → T8 → T10 (schemas → queue → dispatcher worker → supervisor)

## Context
### Original Request
Build Phase 2 of the MegaETH event-driven orchestration system — the infrastructure layer connecting cognitive agents (LangGraph, Phase 3) to on-chain execution. Three structural files: `app/schemas/intent.py` (agent output payload), `app/state/models.py` (user asset state), `app/agents/dispatcher.py` (low-latency transaction dispatch daemon).

### Interview Summary
- **Scope**: Infrastructure-only (Option A). No LangGraph agents, no LLM integration. Schema + queue + worker pool + Transfer event parser.
- **LLM**: Deferred to Phase 3.
- **Asset tracking**: Schema + repository + lightweight ERC-20 Transfer event parser (topic hash matching, standard ABI decoding). Delta-only balances (no on-chain `balanceOf` bootstrap).
- **Private key**: `PRIVATE_KEY` env var (dev); cloud secret manager in production.
- **Test strategy**: Tests-after, integration-heavy. Unit tests for schema validation, integration tests for dispatcher pipeline and balance tracking.
- **GraphQL**: Deferred to Phase 3 (no consumer). `MEGAETH_HISTORICAL_GRAPHQL_URL` env var added for future use. Vector search service also deferred.

### Metis Review
**Critical findings incorporated:**
1. **Queue pattern**: Use Redis STREAM (not LIST) — XREADGROUP/XACK/XAUTOCLAIM, matching Phase 1 patterns for at-least-once delivery, dead-letter routing, and orphan recovery.
2. **`call_data` format**: Explicitly defined as raw ABI-encoded `0x`-prefixed hex. The dispatcher signs and broadcasts — no encoding.
3. **Trigger conditions**: Phase 2 only evaluates `always`. `price_threshold` schema exists but evaluation is stubbed (no oracle).
4. **Slippage**: Schema enforces max 100 bps. Actual slippage computation requires a price feed — deferred.
5. **New `RPC_URL` env var**: Separate HTTP RPC endpoint for `eth_sendRawTransactionSync` (not derivable from `WS_URL`).
6. **Nonce management**: `asyncio.Lock` + `eth_getTransactionCount` per worker. No centralized nonce service.
7. **Address normalization**: Lowercase `0x`-prefixed everywhere.
8. **Deferred files**: `app/data/subgraph_client.py` and `app/vector/search_service.py` moved to Phase 3.

**Risks flagged (to validate during implementation):**
- `eth_sendRawTransactionSync` (EIP-7966) availability on MegaETH RPC — fall back to `w3.provider.make_request()` if no native web3.py support.
- Mini-block receipt format for log extraction — currently `list[Any]`; must verify actual structure at parse time.
- Balance deltas are relative to first observed event in pipeline history — documented limitation.

## Work Objectives
### Core Objective
Build the Phase 2 infrastructure pipeline: Agent Intent JSON → Redis Execution Stream → Async Worker Pool → MegaETH Transaction Broadcast. Plus real-time asset balance tracking via ERC-20 Transfer event extraction from the existing Phase 1 ingestion pipeline.

### Concrete Deliverables
1. **`app/schemas/intent.py`**: `ExecutionPayload` and `TriggerCondition` Pydantic schemas with validation (slippage cap, gas limits, priority enum)
2. **`app/state/models.py`**: `AssetBalance` and `TokenTransfer` SQLModel tables with composite PKs
3. **`app/agents/dispatch.py`**: `ExecutionQueue` (Redis STREAM with consumer groups — XADD/XREADGROUP/XACK/XAUTOCLAIM/dead-letter)
4. **`app/state/transfer_parser.py`**: ERC-20 Transfer event extractor from mini-block receipt logs
5. **`app/state/balance_repository.py`**: `AssetBalanceRepository` (upsert, get_portfolio, get_balance, apply_deltas_and_upsert)
6. **`app/state/balance_worker.py`**: `BalanceTrackerWorker` (consumes from Phase 1 stream, extracts transfers, upserts balances)
7. **`app/agents/dispatcher.py`**: `TransactionExecutor` (sign + broadcast) + `WorkerPool` (single worker consuming from execution stream)
8. **`app/api/dependencies.py`**: Populated with Phase 2 dependencies (DB session factory, Redis client, compiled graph placeholder)
9. **Supervisor integration**: BalanceTrackerWorker + WorkerPool added to `app/api/main.py` task list
10. **Config + metrics**: New env vars, Prometheus counters

### Definition of Done
- [ ] All Pydantic schemas validate with `extra="forbid"`, enforce slippage cap
- [ ] ExecutionQueue uses Redis STREAM with consumer groups (XREADGROUP/XACK/XAUTOCLAIM), dead-letter stream
- [ ] WorkerPool starts/stops cleanly via supervisor, handles shutdown signal without hanging
- [ ] ERC-20 Transfer parser extracts correct deltas from receipt logs, skips non-Transfer topics gracefully
- [ ] AssetBalanceRepository upserts idempotently (ON CONFLICT DO UPDATE)
- [ ] TransactionExecutor signs and broadcasts to local Anvil (or testnet) successfully
- [ ] ~40 tests pass (unit: schemas + parser; integration: dispatcher + balances + balance worker)
- [ ] ruff clean, mypy clean

### Must Have
- Pydantic `ExecutionPayload` with hard-coded max 1% slippage (100 bps) enforced at schema level
- Redis STREAM execution queue with consumer groups (consistency with Phase 1)
- `asyncio.Semaphore` concurrency control in WorkerPool
- `asyncio.Event` graceful shutdown (matching Phase 1 worker pattern)
- Module-level Prometheus counters (`dispatcher_intents_*`, `transfer_events_*`, `asset_balances_*`)
- `PRIVATE_KEY` validation on startup (0x-prefixed, 64 hex chars)
- Address normalization to lowercase `0x`-prefixed

### Must NOT Have (Guardrails)
- **MUST NOT** implement LangGraph agents or LLM calls (Phase 3)
- **MUST NOT** query on-chain `balanceOf` for absolute balances (delta-only from Transfer events)
- **MUST NOT** simulate transactions before broadcast (agent responsible for correctness)
- **MUST NOT** implement multi-signer or HD wallet support (single `PRIVATE_KEY`)
- **MUST NOT** call subgraph client or vector search service from any Phase 2 code (no consumer exists)
- **MUST NOT** handle non-ERC-20 events (ERC-721, ERC-1155, approvals beyond mint/burn)
- **MUST NOT** introduce Redis keys without `megaeth:` prefix
- **MUST NOT** implement fallback RPC providers or multi-chain support (single endpoint)

## Verification Strategy
**Test approach**: Tests-after, integration-heavy.
- **Unit tests**: Pydantic schema validation (valid/invalid payloads, edge cases), Transfer parser (match/miss/decode)
- **Integration tests**: Dispatcher pipeline (inject JSON to Redis Stream → verify worker dequeues and processes), asset balance upsert/query/delta accumulation
- **Failure path tests**: Invalid payloads, gas cap exceeded, slippage cap, Redis disconnection, malformed logs
- **No LLM mocking needed** — no agents built in Phase 2
- **Same isolation pattern as Phase 1**: dedicated Redis test streams, DB truncation, real Docker services

All verification is **agent-executed** — zero manual QA intervention.

## Execution Strategy
### Parallel Execution Waves

```
Wave 1 ──────► Wave 2 ──────► Wave 3 ──────────► Wave 4 ──► Wave 5
                                                              
T1 (intent)    T4 (queue)     T7 (balance worker)    T10 (supv)   T11 (unit tests)
T2 (models)    T5 (parser)    T8 (dispatcher)        T9 (deps)    T12 (int tests)
T3 (config)    T6 (repo)      T9 (deps)
```

### Dependency Matrix

| Task | Blocks | Blocked By | Wave |
|------|--------|------------|------|
| T1 | T4, T8 | — | 1 |
| T2 | T5, T6 | — | 1 |
| T3 | T4, T9 | — | 1 |
| T4 | T8 | T1, T3 | 2 |
| T5 | T11 | T2 | 2 |
| T6 | T12 | T2 | 2 |
| T7 | T10 | T2, T5, T6 | 3 |
| T8 | T10 | T1, T4 | 3 |
| T9 | T12 | T3, T4 | 3 |
| T10 | T12 | T7, T8 | 4 |
| T11 | — | T5 | 5 |
| T12 | — | T6, T9, T10 | 5 |

### Agent Dispatch Summary
- **Wave 1** (3 tasks): All parallel — schemas + config, no dependencies. `sisyphus-junior` with `unspecified-high` rigor.
- **Wave 2** (3 tasks): All parallel — backend services. Each touches 1-2 files. `sisyphus-junior`.
- **Wave 3** (3 tasks): Parallel after Wave 2 — balance worker, dispatcher worker, API dependencies. `sisyphus-junior`.
- **Wave 4** (1 task): Supervisor integration. Depends on both workers (T7 + T8). `sisyphus-junior`.
- **Wave 5** (2 tasks): Tests. `sisyphus-junior` with `unspecified-high` rigor for integration tests.


## TODOs

- [ ] 1. `app/schemas/intent.py` — ExecutionPayload & TriggerCondition Pydantic Schemas
  **What to do**: Create `app/schemas/intent.py` with frozen Pydantic models that define the JSON contract between future LangGraph agents and the execution worker pool.
  - `ExecutionPayload`: target_contract (0x-address), call_data (0x-prefixed ABI-encoded hex), max_gas_price (int, default 1_000_000), trigger_condition (TriggerCondition), max_slippage_bps (int, default 100, max 100), priority (Literal["normal","high","low"]), created_by_agent (str), id (UUID auto-generated), created_at (float auto-time)
  - `TriggerCondition`: condition_type (Literal["always","price_threshold","time_bound"]), params (dict, default empty)
  - Validate: max_slippage_bps ≤ 100, call_data starts with 0x, target_contract is 0x-prefixed 42-char address, priority in allowed set, condition_type in allowed set
  - Export from `app/schemas/__init__.py`
  - Use `from __future__ import annotations` and complete type hints throughout.
  **Must NOT do**: Add price oracle logic. `price_threshold` and `time_bound` conditions have schema definitions but evaluation is stubbed in the dispatcher.
  **Recommended Agent Profile**: `sisyphus-junior` — well-defined Pydantic model creation, follows existing Phase 1 schema pattern.
  **Parallelization**: Wave 1, no blockers, no dependents (blocks T4).
  **References**: `app/schemas/mini_block.py` (frozen model pattern, field_validator for hex, ConfigDict extra="forbid"), `app/schemas/__init__.py` (exports)
  **Acceptance Criteria**:
  - `ExecutionPayload.model_validate(valid_json)` succeeds
  - `max_slippage_bps=101` raises ValidationError
  - `max_slippage_bps=0` is valid (no slippage allowed)
  - Missing `target_contract` raises ValidationError
  - Extra unknown field raises ValidationError
  - `call_data` with non-hex chars raises ValidationError
  - `target_contract` with wrong length raises ValidationError
  **QA Scenarios**:
  1. **Happy path — valid payload**: Tool: `Bash (uv run python)`. Steps: `from app.schemas.intent import ExecutionPayload, TriggerCondition; p = ExecutionPayload(target_contract="0x402085c248EeA27D92E8b30b2C58ed07f9E20001", call_data="0xdeadbeef", trigger_condition=TriggerCondition(condition_type="always"))`. Expected: model created, `p.max_slippage_bps == 100`, `p.priority == "normal"`. Evidence: `.sisyphus/evidence/task-1-valid-payload.txt`
  2. **Failure — slippage cap exceeded**: Tool: `Bash (uv run python)`. Steps: `ExecutionPayload(target_contract="0x402085c248EeA27D92E8b30b2C58ed07f9E20001", call_data="0xdeadbeef", max_slippage_bps=101, trigger_condition=TriggerCondition(condition_type="always"))`. Expected: ValidationError with "slippage" in message. Evidence: `.sisyphus/evidence/task-1-slippage-cap.txt`

- [ ] 2. `app/state/models.py` — AssetBalance & TokenTransfer SQLModel Schemas
  **What to do**: Create `app/state/models.py` with SQLModel table definitions for real-time user asset state tracking.
  - `AssetBalance`: composite PK `(user_address, token_address)`, both `str(42)` lowercase. Fields: `token_symbol` (indexed), `balance_raw` (int, default 0), `decimals` (int, default 18), `block_number` (int, last block that updated this row), `created_at` (DateTime with server_default now()), `updated_at` (DateTime with server_default + onupdate). Property: `balance_human` (raw / 10^decimals).
  - `TokenTransfer`: raw event log table. Auto-increment `id` PK. Fields: `block_number` (indexed), `tx_index`, `log_index`, `token_address` (indexed), `from_address` (indexed), `to_address` (indexed), `amount` (int), `ingested_at` (DateTime with server_default now()).
  - Create tables via `SQLModel.metadata.create_all()` in the state tracker worker init — add these models to the existing `_init_db()` function in `app/state/worker.py`.
  - Export from `app/state/__init__.py` if it exists, or create one.
  - Use `from __future__ import annotations` and complete type hints throughout.
  **Must NOT do**: Add TokenMetadata table. Handle non-ERC-20 events. Add chain_id to PK (future need, not Phase 2).
  **Recommended Agent Profile**: `sisyphus-junior` — SQLModel table creation, follows existing `MiniBlockRecord` pattern.
  **Parallelization**: Wave 1, no blockers, no dependents (blocks T5, T6).
  **References**: `app/schemas/state.py` (MiniBlockRecord pattern — composite PK, DateTime server_default, JSON column), `app/state/database.py` (_init_db pattern), `app/state/worker.py` (where to add create_all)
  **Acceptance Criteria**:
  - `AssetBalance` table created in PostgreSQL via SQLModel.metadata.create_all()
  - Composite PK `(user_address, token_address)` enforced — duplicate upsert creates 1 row
  - `block_number` and `updated_at` auto-initialized on insert
  - `TokenTransfer` table created with auto-increment id
  **QA Scenarios**:
  1. **Table creation**: Tool: `Bash (uv run python)`. Steps: Run `_init_db()` from state tracker, then `SELECT count(*) FROM asset_balances`. Expected: 0 rows, no error. Evidence: `.sisyphus/evidence/task-2-table-create.txt`
  2. **Composite PK enforcement**: Tool: `Bash (uv run python)`. Steps: Insert two AssetBalance rows with same (user_address, token_address) via upsert. Query count. Expected: 1 row. Evidence: `.sisyphus/evidence/task-2-composite-pk.txt`

- [ ] 3. Config & Metrics — New Environment Variables & Prometheus Counters
  **What to do**: Extend `app/core/config.py` and `app/core/metrics.py` for Phase 2.
  - **New Settings**: `rpc_url` (`str`, default `"http://127.0.0.1:8545"`), `private_key` (`str`, default `""` — validated on worker startup), `megaeth_historical_graphql_url` (`str`, default `""`), `execution_stream` (`str`, default `"megaeth:stream:executions"`), `execution_consumer_group` (`str`, default `"megaeth:workers:executors"`), `execution_dead_letter` (`str`, default `"megaeth:stream:executions:dead"`), `worker_pool_count` (`int`, default 1 — Phase 2 constrains to single worker to avoid nonce collisions with one private key), `max_concurrent_tx` (`int`, default 10), `balance_consumer_group` (`str`, default `"megaeth:workers:balances"`)
  - **New Metrics**: `dispatcher_intents_received_total`, `dispatcher_intents_executed_total`, `dispatcher_intents_failed_total`, `dispatcher_intents_rejected_total`, `transfer_events_parsed_total`, `transfer_events_failed_total`, `transfer_events_skipped_total`, `asset_balances_upserted_total`, `asset_balances_queried_total`, `execution_queue_depth` — all module-level `int` with `# type: ignore[assignment]` pattern from existing metrics
  - **Update `.env`**: Add `RPC_URL=http://127.0.0.1:8545`, `PRIVATE_KEY=` (empty for safety), `MEGAETH_HISTORICAL_GRAPHQL_URL=` (empty, for Phase 3)
  - **Update `.env.example`**: Same additions with comments explaining each variable
  **Must NOT do**: Add WebSocket config for execution (HTTP RPC only). Add subgraph client code.
  **Recommended Agent Profile**: `sisyphus-junior` — config extension, metric counter addition, follows existing patterns exactly.
  **Parallelization**: Wave 1, no blockers, no dependents (blocks T4, T8).
  **References**: `app/core/config.py` (Settings class, field defaults, .env loading), `app/core/metrics.py` (counter naming pattern, type: ignore), `.env` (current entries), `.env.example`
  **Acceptance Criteria**:
  - `settings.rpc_url` resolves to `"http://127.0.0.1:8545"` by default
  - All new metric counters initialized to 0 and importable
  - `.env` and `.env.example` have new entries with comments
  **QA Scenarios**:
  1. **Config resolution**: Tool: `Bash (uv run python)`. Steps: `from app.core.config import settings; print(settings.rpc_url, settings.execution_stream)`. Expected: default values printed. Evidence: `.sisyphus/evidence/task-3-config.txt`
  2. **Metrics importable**: Tool: `Bash (uv run python)`. Steps: `from app.core.metrics import dispatcher_intents_received_total; print(dispatcher_intents_received_total)`. Expected: `0`. Evidence: `.sisyphus/evidence/task-3-metrics.txt`

- [ ] 4. `app/agents/dispatch.py` — ExecutionQueue with Redis STREAM
  **What to do**: Create `app/agents/dispatch.py` with `ExecutionQueue` class following the Phase 1 `StreamBuffer` + `StreamConsumer` pattern for the execution stream.
  - `ExecutionQueue` class: `__init__(redis_client)` with lazy connection pattern. Constants: `STREAM_KEY = "megaeth:stream:executions"`, `GROUP_NAME = "megaeth:workers:executors"`, `DEAD_LETTER = "megaeth:stream:executions:dead"`, `RESULT_PREFIX = "megaeth:executions:"`
  - `initialize()`: Create execution stream + dead-letter stream (XADD init marker + XDEL, XTRIM to MAXLEN ~100K). Create consumer group (XGROUP CREATE $ MKSTREAM, handle BUSYGROUP).
  - `enqueue(payload: ExecutionPayload) -> str`: Serialize to JSON, XADD to execution stream with fields: `payload` (JSON string), `created_at` (float), `created_by` (agent name). Return stream entry ID.
  - `consume(block_ms: int = 5000) -> AsyncIterator[tuple[str, ExecutionPayload]]`: XREADGROUP with `>` (new messages), count=5, block=block_ms. On NOGROUP error, auto-create group at `$`. Parse `payload` field from JSON. Yield `(entry_id, ExecutionPayload)`.
  - `ack(entry_id: str)`: XACK
  - `claim_pending(min_idle_ms: int = 30000) -> list[tuple[str, ExecutionPayload]]`: XAUTOCLAIM with start_id="0-0", count=50. Parse and return.
  - `dead_letter(entry_id: str, payload: ExecutionPayload, reason: str)`: XADD to dead-letter stream with fields: `original_stream`, `original_entry_id`, `consumer_group`, `payload` (JSON), `reason`, `dead_lettered_at` (float).
  - `set_result(execution_id: str, status: str, tx_hash: str = "", error: str = "")`: HSET to result hash with 24h TTL.
  - `get_result(execution_id: str) -> dict | None`: HGETALL from result hash.
  - Use `from __future__ import annotations` and complete type hints throughout.
  **Must NOT do**: Use Redis LIST (LPUSH/BRPOP). Implement subgraph or vector search logic. Add more than one consumer group.
  **Recommended Agent Profile**: `sisyphus-junior` — follows established Phase 1 patterns exactly (`StreamBuffer.initialize`, `StreamConsumer.consume`, `StreamConsumer.claim_pending`).
  **Parallelization**: Wave 2, blocks T7, blocked by T1 + T3.
  **References**: `app/streams/buffer.py` (initialize pattern: EXISTS check, init marker, XTRIM, create_consumer_group), `app/streams/consumer.py` (consume iterator, parse_entry, dead_letter, claim_pending, NOGROUP handling), `app/schemas/intent.py` (ExecutionPayload model)
  **Acceptance Criteria**:
  - `ExecutionQueue.initialize()` creates stream + consumer group without error on first and second call (idempotent)
  - `ExecutionQueue.enqueue(valid_payload)` returns a valid stream entry ID (e.g., `"1234567890123-0"`)
  - `ExecutionQueue.consume()` yields `(entry_id, ExecutionPayload)` for enqueued messages
  - `ExecutionQueue.ack(entry_id)` removes from PEL
  - `ExecutionQueue.dead_letter(entry_id, payload, "test reason")` writes to dead-letter stream
  - `ExecutionQueue.set_result(id, "completed", tx_hash="0x123")` creates hash; `get_result(id)` returns it
  **QA Scenarios**:
  1. **Happy path — enqueue + consume + ack**: Tool: `Bash (uv run python)`. Steps: Start Redis (docker compose up -d redis). Initialize ExecutionQueue. Enqueue a valid ExecutionPayload. Consume 1 message. Verify entry_id is valid, payload parses correctly. Ack the entry. Verify PEL is empty. Evidence: `.sisyphus/evidence/task-4-enqueue-consume.txt`
  2. **Dead-letter routing**: Tool: `Bash (uv run python)`. Steps: Enqueue a payload. Consume it. Call dead_letter() with reason "test failure". Read dead-letter stream. Expected: 1 entry with matching payload JSON and reason. Evidence: `.sisyphus/evidence/task-4-dead-letter.txt`

- [ ] 5. `app/state/transfer_parser.py` — ERC-20 Transfer Event Extractor
  **What to do**: Create `app/state/transfer_parser.py` with a pure function that extracts ERC-20 Transfer event deltas from mini-block receipt logs.
  - `TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"` (keccak256("Transfer(address,address,uint256)"))
  - `TransferDelta` dataclass or lightweight model: `token_address: str`, `from_address: str`, `to_address: str`, `amount: int`, `block_number: int`, `tx_index: int`, `log_index: int`
  - `extract_transfers(payload: MiniBlockPayload) -> list[TransferDelta]`:
    - Assumed receipt log structure (matching standard Ethereum JSON-RPC format): `{"status": "0x1"|"0x0"|1|0, "logs": [{"topics": [topic0, topic1, topic2], "data": "0x..."}]}`
    - Add defensive `isinstance(receipt, dict)` and `isinstance(log, dict)` checks before dereferencing; skip non-dict entries gracefully
    - Iterate payload.receipts (list of receipt dicts)
    - Skip receipts with `status != 1` (failed transactions) — use `status == "0x1"` for hex and `status == 1` for int
    - For each receipt, iterate `receipt["logs"]`
    - Match `log["topics"][0] == TRANSFER_TOPIC` (case-insensitive)
    - Decode: `from_address = "0x" + log["topics"][1][-40:]` (lowercase), `to_address = "0x" + log["topics"][2][-40:]`, `amount = int(log["data"], 16)` (uint256 from data field)
    - Handle edge cases: topic count < 3 → skip gracefully, data shorter than 64 chars → pad or skip
    - Normalize all addresses to lowercase 0x-prefixed
    - Handle mint (from=0x0) and burn (to=0x0) — still produce deltas (consumer decides whether to track)
    - Handle 0-value transfers — produce zero-amount deltas
    - Skip malformed logs with WARNING log
  - `apply_deltas(deltas: list[TransferDelta]) -> list[tuple[str, str, int]]`: Aggregate deltas into `(user_address, token_address, net_change)` tuples. Positive for recipient, negative for sender. Skip self-transfers (from == to) — net delta zero.
  - All functions are synchronous (pure computation, no I/O).
  - Use `from __future__ import annotations` and complete type hints throughout.
  **Must NOT do**: Handle non-ERC-20 events (ERC-721, ERC-1155, approvals). Query contract ABI externally. Handle rebasing/fee-on-transfer tokens. Implement wallet-level balance tracking (that's the repository's job).
  **Recommended Agent Profile**: `sisyphus-junior` — pure function with well-defined input/output, no external dependencies.
  **Parallelization**: Wave 2, blocks T10, blocked by T2.
  **References**: `app/schemas/mini_block.py` (MiniBlockPayload — receipts field, hex string handling), `app/vector/summarizer.py` (pure function pattern), `app/core/logging.py` (get_logger for warnings)
  **Acceptance Criteria**:
  - Empty mini-block (no transactions) → returns empty list
  - Receipt with 1 Transfer event → returns 1 TransferDelta with correct addresses and amount
  - Receipt with 3 Transfer events across different tokens → returns 3 deltas
  - Receipt with non-Transfer topic (e.g., Approval) → Transfer event skipped, no error
  - Receipt with status=0 (failed) → all logs skipped
  - Malformed log with 2 topics → skipped with WARNING, no crash
  - Transfer with 0x0 to_address (burn) → delta produced
  - Transfer with 0x0 from_address (mint) → delta produced
  **QA Scenarios**:
  1. **Single Transfer parse**: Tool: `Bash (uv run python)`. Steps: Construct MiniBlockPayload with 1 receipt containing 1 Transfer log. Call `extract_transfers()`. Expected: 1 TransferDelta with correct from, to, amount. Evidence: `.sisyphus/evidence/task-5-single-transfer.txt`
  2. **Skip failed transaction**: Tool: `Bash (uv run python)`. Steps: Construct MiniBlockPayload with receipt where `status="0x0"` and a Transfer log. Call `extract_transfers()`. Expected: empty list. Evidence: `.sisyphus/evidence/task-5-failed-tx.txt`

- [ ] 6. `app/state/balance_repository.py` — AssetBalanceRepository
  **What to do**: Create `app/state/balance_repository.py` with `AssetBalanceRepository` class following the Phase 1 `StateRepository` pattern.
  - `AssetBalanceRepository.__init__(self, session: AsyncSession)`
  - `upsert(self, balance: AssetBalance) -> None`: Use native PostgreSQL `INSERT ... ON CONFLICT (user_address, token_address) DO UPDATE` via `sqlalchemy.dialects.postgresql.insert()`. Update: `balance_raw`, `token_symbol`, `block_number`, `updated_at`. Commit after upsert.
  - `get_portfolio(self, user_address: str) -> list[AssetBalance]`: `SELECT * FROM asset_balances WHERE user_address = :addr`. Return all rows for user.
  - `get_balance(self, user_address: str, token_address: str) -> AssetBalance | None`: Single-row lookup via composite PK.
  - `bulk_upsert(self, balances: list[AssetBalance]) -> int`: Bulk insert with ON CONFLICT DO UPDATE using raw SQL text for performance. Return count of upserted rows.
  - `apply_deltas_and_upsert(self, deltas: list[tuple[str, str, int]], block_number: int) -> int`: Read current balances, apply net changes from TransferDelta aggregation, upsert back. If a delta would cause the balance to go negative, clamp to 0 (do not go negative). Log a WARNING. The delta is still partially applied up to the available balance.
  - All methods use `async` (database I/O). Address normalization: always lowercase before DB operations.
  - Use `from __future__ import annotations` and complete type hints throughout.
  - Increment `asset_balances_upserted_total` and `asset_balances_queried_total` metrics on respective operations.
  **Must NOT do**: Use ORM `session.merge()` for bulk operations (Phase 1 used it for single-row upserts; bulk needs native SQL for performance). Add TokenMetadata validation.
  **Recommended Agent Profile**: `sisyphus-junior` — repository pattern, follows existing StateRepository conventions.
  **Parallelization**: Wave 2, blocks T10, blocked by T2.
  **References**: `app/state/repository.py` (async session, upsert pattern, get_latest_block query), `app/state/models.py` (AssetBalance schema), `app/core/metrics.py` (counter increments)
  **Acceptance Criteria**:
  - `upsert()` with new (user, token) pair → 1 row created
  - `upsert()` same pair twice → 1 row with updated balance_raw
  - `get_portfolio("0xabc")` returns all tokens for that user
  - `get_balance("0xabc", "0xdef")` returns single row or None
  - `bulk_upsert()` with 50 rows → all persisted, count=50
  **QA Scenarios**:
  1. **Idempotent upsert**: Tool: `Bash (uv run python)`. Steps: Create AssetBalance(user="0xaaa", token="0xbbb", balance_raw=1000). Upsert twice. Query count WHERE user_address="0xaaa". Expected: 1 row, balance_raw=1000. Evidence: `.sisyphus/evidence/task-6-idempotent-upsert.txt`
  2. **Portfolio query**: Tool: `Bash (uv run python)`. Steps: Upsert balances for user 0xccc with tokens 0xt1 (100), 0xt2 (200), 0xt3 (300). Call get_portfolio("0xccc"). Expected: 3 rows in result. Evidence: `.sisyphus/evidence/task-6-portfolio-query.txt`

- [ ] 7. `app/state/balance_worker.py` — BalanceTrackerWorker (Pipeline Integration)
  **What to do**: Create `app/state/balance_worker.py` with `BalanceTrackerWorker` class that bridges the transfer parser and balance repository into the live Phase 1 ingestion pipeline. This is the **missing integration point** — without it, the parser and repository are dead code.
  - `BalanceTrackerWorker` class following the Phase 1 worker pattern (`StateTrackerWorker`, `VectorIndexerWorker`):
    - `__init__()`: Accept `redis_client`, `stream_name` (default `settings.stream_name` — same stream as ingestion), `consumer_group` (default `settings.balance_consumer_group`, e.g. `"megaeth:workers:balances"`), `dead_letter` (default `"megaeth:raw:miniBlocks:balance_dead"`), `block_ms` (default 1000)
    - `_get_redis()`: Lazy connection from `settings.redis_url`. Match Phase 1 pattern.
    - `_init_db()`: Use sync engine to create AssetBalance + TokenTransfer tables via `SQLModel.metadata.create_all()`. Idempotent.
    - `_init_stream()`: Create consumer group on the main miniBlocks stream (XGROUP CREATE $ MKSTREAM, handle BUSYGROUP). Same stream that StateTrackerWorker and VectorIndexerWorker consume from — just a new consumer group for balance tracking.
    - `start()`: Initialize DB, stream, consumer. Create `StreamConsumer` for the main stream. Spawn `_process_loop` asyncio task.
    - `stop()`: Set shutdown event. Close Redis. Log stop.
    - `_process_loop()`: Main consumption loop. While not shutdown: call consumer.consume(). For each (entry_id, data): parse entry → parse mini_block → extract_transfers → apply_deltas_and_upsert → ack. On error: dead_letter + ack. Log metrics.
  - Use `from __future__ import annotations` and complete type hints throughout.
  - Increment `transfer_events_parsed_total`, `transfer_events_failed_total`, `transfer_events_skipped_total` metrics within the loop.
  **Must NOT do**: Modify StateTrackerWorker or VectorIndexerWorker. Create a new Redis Stream (consumes from existing `megaeth:raw:miniBlocks`). Query on-chain `balanceOf`.
  **Recommended Agent Profile**: `sisyphus-junior` — follows established Phase 1 worker patterns exactly.
  **Parallelization**: Wave 3, blocks T10, blocked by T2 + T5 + T6.
  **References**: `app/state/worker.py` (StateTrackerWorker lifecycle: init_db, init_stream, consume loop, shutdown), `app/state/transfer_parser.py` (extract_transfers), `app/state/balance_repository.py` (apply_deltas_and_upsert), `app/streams/consumer.py` (StreamConsumer class), `app/core/config.py` (settings), `app/core/metrics.py` (counters)
  **Acceptance Criteria**:
  - BalanceTrackerWorker creates consumer group on main miniBlocks stream (idempotent)
  - Injected mini-block with Transfer events → worker extracts transfers, upserts balances
  - Injected mini-block with no Transfer events → worker skips, acks, no error
  - Worker handles malformed data gracefully (dead_letter + ack, continues)
  - Worker shutdown via asyncio.Event: clean exit within 10s
  **QA Scenarios**:
  1. **Balance tracking from mini-block**: Tool: `Bash (uv run python)`. Prerequisites: Docker services running. Steps: Start BalanceTrackerWorker with test stream. Inject a MiniBlockPayload with 1 Transfer event (user_a → user_b, token=0xt1, amount=500). Wait 5s. Query AssetBalance table for both addresses. Expected: user_a balance=-500, user_b balance=+500 for token 0xt1. Evidence: `.sisyphus/evidence/task-7-balance-track.txt`
  2. **Empty block skip**: Tool: `Bash (uv run python)`. Steps: Inject MiniBlockPayload with 0 transactions. Query stream consumer PEL. Expected: message XACKed, no asset balances created, no errors. Evidence: `.sisyphus/evidence/task-7-empty-block.txt`

- [ ] 8. `app/agents/dispatcher.py` — TransactionExecutor & WorkerPool (Single Worker)
  **What to do**: Create `app/agents/dispatcher.py` containing the low-latency transaction dispatch daemon that consumes execution plans from the Redis STREAM and broadcasts them to MegaETH.
  - **TransactionExecutor class**:
    - `__init__()`: Accept optional web3 provider. Lazy-init `AsyncWeb3` from `settings.rpc_url` on first use. Load `PRIVATE_KEY` from `settings.private_key` env var, validate format (0x-prefixed, 64 hex chars) on startup. Derive `from_address` from private key.
    - `_get_w3() -> AsyncWeb3`: Lazy connection to HTTP RPC.
    - `_get_nonce(with_lock: asyncio.Lock) -> int`: Get nonce via `w3.eth.get_transaction_count(from_address)` on first call per session, then increment counter. Protected by per-instance `asyncio.Lock`. (Nonce counter reset on executor restart.)
    - `_check_conditions(condition: TriggerCondition) -> bool`: Validate trigger. Phase 2 only evaluates `always` → returns True. `price_threshold` and `time_bound` return False with WARNING log (stub).
    - `_check_slippage(payload: ExecutionPayload) -> bool`: Schema-level enforcement (max_slippage_bps ≤ 100). Phase 2 does not compute actual slippage (no price feed). Returns True always.
    - `_sign_transaction(tx: dict, w3: AsyncWeb3) -> str`: Use `w3.eth.account.sign_transaction(tx, private_key)` then `rawTransaction.hex()`.
    - `_broadcast(signed_tx_hex: str, w3: AsyncWeb3) -> str`: Try `w3.eth.send_raw_transaction_sync(signed_tx_hex)` if available on web3.py 7.x. Fall back to `w3.provider.make_request("eth_sendRawTransactionSync", [signed_tx_hex])`. Fall back to `w3.eth.send_raw_transaction(signed_tx_hex)` if sync variant unavailable. Return tx_hash.
    - `execute(payload: ExecutionPayload) -> dict`: Full execution pipeline: check conditions → check slippage → assemble transaction dict (from, to=target_contract, data=call_data, gasPrice=max_gas_price, gas=hardcoded gas limit ~500K) → get nonce → sign → broadcast → return {"status": "success", "tx_hash": "0x..."}. Increment metrics counters. On failure: return {"status": "failed"/"rejected"/"skipped", "reason": "..."} with appropriate metric.
  - **WorkerPool class**:
    - `__init__(worker_count: int = 1, max_concurrent_tx: int = 10)`: Initialize from settings. **MUST default to 1 worker** — with a single `PRIVATE_KEY`, multiple concurrent workers using the same key will produce nonce collisions (two workers signing with the same nonce). Multi-worker dispatch (with different keys or a shared nonce manager) is deferred to a future phase. Create `asyncio.Event` for shutdown (expose via public `shutdown_event` property returning `self._shutdown_event`), `asyncio.Semaphore(max_concurrent_tx)`, `list[asyncio.Task]` for workers, `ExecutionQueue` instance.
    - `_get_redis() -> redis.Redis`: Lazy connection from `settings.redis_url`. Match Phase 1 `StreamBuffer._get_redis` pattern.
    - `start()`: Initialize ExecutionQueue (create stream + consumer group + dead-letter). Spawn `worker_count` asyncio tasks via `asyncio.create_task(self._worker(i))`. Log startup.
    - `stop()`: Set shutdown event. Wait for all worker tasks with 30s timeout (gather with return_exceptions=True). Cancel on timeout. Close Redis. Log stop.
    - `_worker(worker_id: int)`: Main loop. While not shutdown: call `queue.consume(block_ms=1000)`. For each (entry_id, payload): acquire semaphore, execute via TransactionExecutor, set result, ack. On ExecutionPayload parse error: dead_letter + ack. On execute error: set error result, ack. On Redis error: backoff 1s. Log all state transitions.
    - `_claim_orphans()`: On startup, XAUTOCLAIM pending messages with min_idle_ms=60000. Re-process or dead-letter with "orphaned" reason.
  - Use `from __future__ import annotations` and complete type hints throughout.
  **Must NOT do**: Call web3.py from any agent code (this IS the worker — it's allowed). Implement price oracle or actual slippage computation. Use multi-signer keys. Implement gas estimation (use hardcoded gas limits per MegaETH cheat sheet). Use worker_count > 1 in Phase 2.
  **Recommended Agent Profile**: `sisyphus-junior` — follows established Phase 1 worker patterns (StateTrackerWorker, VectorIndexerWorker) with web3.py integration.
  **Parallelization**: Wave 3, blocks T10, blocked by T1 + T4.
  **References**: `app/state/worker.py` (worker lifecycle: init, claim_pending, consume loop, shutdown), `app/streams/consumer.py` (consume pattern, XACK, dead_letter), `app/agents/dispatch.py` (ExecutionQueue), `app/schemas/intent.py` (ExecutionPayload), `app/core/config.py` (settings), `app/core/metrics.py` (counters), `app/core/logging.py` (get_logger), AGENTS.md (MegaETH gas model — 0.001 gwei base fee, hardcode gas limits, eth_sendRawTransactionSync)
  **Acceptance Criteria**:
  - WorkerPool starts 1 worker (single-worker constraint), consuming from execution stream
  - Enqueue valid ExecutionPayload → worker dequeues, executes, sets result hash with tx_hash
  - Enqueue payload with max_slippage_bps=101 → rejected at schema level before reaching dispatcher
  - WorkerPool.stop() triggers clean shutdown within 30s
  - Orphaned messages (pending >60s) reclaimed on startup via XAUTOCLAIM
  - Invalid JSON in stream → dead_letter + ack, worker continues
  - RPC unreachable → worker retries with backoff, doesn't crash
  - `worker_pool.shutdown_event` is publicly accessible (not `._shutdown`)
  **QA Scenarios**:
  1. **End-to-end dispatch**: Tool: `Bash (uv run python)`. Prerequisites: Anvil running (`anvil --chain-id 6343 --gas-price 1000000 --block-time 1 --accounts 10 --balance 10000`), Redis running, private key set to Anvil account #0. Steps: Start WorkerPool (1 worker). Enqueue valid ExecutionPayload targeting Anvil itself (any address, 0 ETH transfer via empty call_data). Wait 5s. Query result hash. Expected: status="success", tx_hash is valid 0x-prefixed hash. Evidence: `.sisyphus/evidence/task-8-e2e-dispatch.txt`
  2. **Graceful shutdown**: Tool: `Bash (uv run python)`. Steps: Start WorkerPool (1 worker). Wait 2s. Call pool.stop(). Verify all tasks complete within 10s. Expected: no hanging tasks, clean exit. Evidence: `.sisyphus/evidence/task-8-shutdown.txt`

- [ ] 9. `app/api/dependencies.py` — Populate Phase 2 Dependencies & Health Checks
  **What to do**: Replace the placeholder `app/api/dependencies.py` with real FastAPI dependency-injection callables for Phase 2.
  - `get_db_session_factory() -> async_sessionmaker[AsyncSession]`: Return DB session factory from `app.state.database` (cached singleton). Match existing engine creation pattern.
  - `get_redis_client() -> redis.asyncio.Redis`: Return async Redis client from `settings.redis_url` with `decode_responses=True`.
  - `get_execution_queue() -> ExecutionQueue`: Return initialized ExecutionQueue (singleton — initialize once, reuse).
  - `get_worker_pool() -> WorkerPool`: Return WorkerPool instance (not started — caller manages lifecycle).
  - `get_agent_graph()`: Placeholder returning `None` (Phase 3 will return compiled LangGraph graph). Document as reserved.
  - **Health check updates in `app/api/routes/`**: Add RPC connectivity check to `/health/ready`: call `eth_blockNumber` via httpx to `settings.rpc_url`. Add execution stream check: verify Redis stream exists and is reachable. Both checks report "healthy"/"degraded"/"unhealthy" with appropriate HTTP status codes.
  - Use `functools.lru_cache` or module-level singletons for cached dependencies.
  - Use `from __future__ import annotations` and complete type hints throughout.
  **Must NOT do**: Implement agent graph. Add authentication middleware. Add new API routes beyond health checks.
  **Recommended Agent Profile**: `sisyphus-junior` — dependency injection setup, FastAPI patterns, follows existing `app/api/main.py` and `app/api/routes/` conventions.
  **Parallelization**: Wave 3, blocks T12, blocked by T3 + T4.
  **References**: `app/api/main.py` (FastAPI app creation, health routes), `app/state/database.py` (engine pool creation), `app/agents/dispatch.py` (ExecutionQueue), `app/agents/dispatcher.py` (WorkerPool), `app/state/repository.py` (session usage pattern)
  **Acceptance Criteria**:
  - `get_db_session_factory()` returns valid async_sessionmaker when PostgreSQL is running
  - `get_redis_client()` returns connected Redis client
  - `get_execution_queue()` returns initialized ExecutionQueue (stream + consumer group created)
  - `/health/ready` reports RPC and execution stream status
  **QA Scenarios**:
  1. **Health check verifies RPC**: Tool: `Bash (curl)`. Prerequisites: FastAPI server running. Steps: `curl http://127.0.0.1:8080/health/ready`. Expected: JSON response with `rpc` and `execution_stream` keys, status values "healthy"/"degraded". Evidence: `.sisyphus/evidence/task-8-health-check.txt`

- [ ] 10. Supervisor Integration — Add Both Workers to `app/api/main.py`
  **What to do**: Extend the existing `supervise()` function in `app/api/main.py` to launch the Phase 2 workers alongside Phase 1 components.
  - Import `WorkerPool` from `app.agents.dispatcher` and `BalanceTrackerWorker` from `app.state.balance_worker`
  - In `supervise()`, after the Phase 1 tasks (ingestion, state, vector): instantiate `WorkerPool(worker_count=settings.worker_pool_count)` and `BalanceTrackerWorker()`, call `await worker_pool.start()` and `await balance_worker.start()`, add shutdown trackers: `asyncio.create_task(worker_pool.shutdown_event.wait())` and `asyncio.create_task(balance_worker._shutdown_event.wait())` to the tasks list
  - Add graceful shutdown handling: on SIGTERM/SIGINT, call `worker_pool.stop()` and `balance_worker.stop()` before cancelling other tasks
  - Add startup log messages: `logger.info("Phase 2 worker pool started", worker_count=settings.worker_pool_count)` and `logger.info("Phase 2 balance worker started")`
  - Update the docstring to list Phase 2 components (BalanceTrackerWorker, WorkerPool)
  **Must NOT do**: Change Phase 1 component lifecycle. Remove existing shutdown signal handlers. Add agent graph invocation. Access private attribute `._shutdown` on WorkerPool (use public `shutdown_event` property).
  **Recommended Agent Profile**: `sisyphus-junior` — single file edit, well-defined integration point.
  **Parallelization**: Wave 4, blocks T12, blocked by T7 + T8.
  **References**: `app/api/main.py` (supervise function, task list, signal handlers), `app/agents/dispatcher.py` (WorkerPool), `app/state/balance_worker.py` (BalanceTrackerWorker), `app/core/config.py` (settings.worker_pool_count)
  **Acceptance Criteria**:
  - `uv run python -m app.api.main` starts 5 supervisor tasks (ingestion, state, vector, balance worker, dispatcher worker pool)
  - Both Phase 2 workers appear in startup log
  - SIGTERM triggers WorkerPool.stop() + BalanceTrackerWorker.stop() alongside Phase 1 shutdown
  **QA Scenarios**:
  1. **Supervisor starts with both workers**: Tool: `Bash (timeout 15s)`. Prerequisites: Docker services running, Anvil running. Steps: `timeout 10 uv run python -m app.api.main 2>&1 | grep -i "balance worker\|worker pool"`. Expected: "Phase 2 balance worker started" and "Phase 2 worker pool started" in log output. Evidence: `.sisyphus/evidence/task-10-supervisor-startup.txt`

- [ ] 11. Unit Tests — Schema Validation & Transfer Parser
  **What to do**: Create `tests/unit/test_intent_schemas.py` and `tests/unit/test_transfer_parser.py`.
  - **`test_intent_schemas.py`** (6-8 tests):
    - `test_valid_execution_payload` — all fields valid → model created
    - `test_slippage_cap_enforced` — max_slippage_bps=101 → ValidationError
    - `test_slippage_zero_valid` — max_slippage_bps=0 → valid
    - `test_missing_required_field` — no target_contract → ValidationError
    - `test_extra_field_rejected` — unknown field → ValidationError (extra="forbid")
    - `test_invalid_call_data` — call_data without 0x prefix → ValidationError
    - `test_invalid_priority` — priority="urgent" → ValidationError
    - `test_invalid_condition_type` — condition_type="unknown" → ValidationError
    - `test_default_values` — min fields → defaults applied (max_gas_price, max_slippage_bps, priority)
  - **`test_transfer_parser.py`** (8-10 tests):
    - `test_empty_block` — no transactions → empty list
    - `test_single_transfer` — 1 Transfer log → 1 delta with correct addresses and amount
    - `test_multiple_transfers` — 3 Transfer logs across different tokens → 3 deltas
    - `test_non_transfer_topic` — Approval log → skipped, no error
    - `test_failed_transaction` — receipt status=0 → all logs skipped
    - `test_malformed_log_two_topics` — topic count < 3 → skipped, no crash
    - `test_burn_event` — to_address=0x0000 → delta produced with zero_address recipient
    - `test_mint_event` — from_address=0x0000 → delta produced with zero_address sender
    - `test_self_transfer` — from == to → delta zero, optionally skipped
    - `test_address_normalization` — uppercase in log → lowercase in output
    - `test_apply_deltas_aggregation` — same user/token across multiple transfers → net change correct
  - Use `pytestmark = pytest.mark.asyncio` if needed. Import from `app.schemas.intent`, `app.state.transfer_parser`, `app.state.models`.
  - Mock mini-block receipts with proper log structure: `{"status": "0x1", "logs": [{"topics": ["0xddf252ad...", "0x000...from", "0x000...to"], "data": "0x" + amount_hex}]}`
  **Must NOT do**: Test dispatcher or database (integration tests). Mock LLM. Mock Redis.
  **Recommended Agent Profile**: `sisyphus-junior` — standard pytest, pure function tests, no external services.
  **Parallelization**: Wave 5, no blockers (can run in parallel with T12).
  **References**: `tests/unit/test_schemas.py` (Pydantic validation test patterns), `tests/unit/test_summarizer.py` (pure function test patterns), `tests/unit/test_daemon.py` (unit test structure), `app/schemas/intent.py`, `app/state/transfer_parser.py`
  **Acceptance Criteria**: All 14-18 unit tests pass when run with `uv run -m pytest tests/unit/test_intent_schemas.py tests/unit/test_transfer_parser.py -v`
  **QA Scenarios**:
  1. **All unit tests pass**: Tool: `Bash (uv run pytest)`. Steps: `uv run -m pytest tests/unit/test_intent_schemas.py tests/unit/test_transfer_parser.py -v`. Expected: all tests green, 0 failures. Evidence: `.sisyphus/evidence/task-11-unit-tests.txt`

- [ ] 12. Integration Tests — Dispatcher Pipeline, Asset Balances & Balance Worker
  **What to do**: Create `tests/integration/test_dispatcher.py`, `tests/integration/test_asset_balances.py`, and `tests/integration/test_balance_worker.py`.
  - **`test_dispatcher.py`** (8-10 tests):
    - `test_dispatcher_picks_up_intent` — enqueue payload to execution stream → worker dequeues within 5s
    - `test_dispatcher_writes_result` — after dequeuing and executing → result hash has status and tx_hash
    - `test_dispatcher_rejects_invalid_json` — malformed JSON in stream → dead_letter + ack, worker continues
    - `test_dispatcher_slippage_cap_enforced` — payload with 101 bps → rejected at schema level (test schema, not dispatcher)
    - `test_dispatcher_multiple_intents_sequential` — enqueue 5 intents → all processed in order
    - `test_dispatcher_graceful_shutdown` — start worker, enqueue 1 intent, signal stop() → worker drains current intent, exits cleanly within 15s
    - `test_dispatcher_dead_letter_orphan` — enqueue, don't ack, wait >60s → XAUTOCLAIM reclaims on new worker start
    - `test_execution_queue_idempotent_init` — initialize twice → no error, stream exists
    - `test_execution_queue_consume_acks` — enqueue → consume → ack → PEL empty
  - **`test_asset_balances.py`** (6-8 tests):
    - `test_upsert_new_balance` — new (user, token) pair → 1 row
    - `test_upsert_idempotent` — same pair twice → 1 row, second overwrites
    - `test_get_portfolio` — upsert 3 tokens for user → get_portfolio returns 3 rows
    - `test_get_balance_single` — upsert 1 → get_balance returns it; unknown pair returns None
    - `test_bulk_upsert` — bulk_upsert 30 rows → count=30, all queryable
    - `test_delta_application` — apply deltas of +100 and -50 for same (user, token) → net balance 50
    - `test_address_case_normalization` — upsert with "0xABC" → query with "0xabc" → row found (same PK)
    - `test_negative_balance_clamped` — delta would make balance negative → balance clamped to 0 with warning (NOT delta-skipped; balance is 0, not previous value)
  - **`test_balance_worker.py`** (4-6 tests):
    - `test_worker_creates_consumer_group` — start worker → consumer group exists on main stream
    - `test_worker_extracts_transfer_and_upserts` — inject mini-block with 1 Transfer → balance row created for both addresses
    - `test_worker_skips_empty_block` — inject mini-block with no transactions → no balance rows, XACKed
    - `test_worker_handles_malformed_receipt` — inject malformed log → skipped with warning, worker continues
    - `test_worker_shutdown_clean` — start worker, inject 1 block, stop() → clean exit within 10s
    - `test_worker_idempotent_upsert` — inject same transfer twice → single balance row, second overwrites
  - Use Phase 1 test patterns: `pytestmark = pytest.mark.asyncio`, dedicated Redis test streams (`megaeth:test:execution:*` for dispatcher, `megaeth:test:miniBlocks` for balance worker), DB TRUNCATE before/after, `@pytest_asyncio.fixture` for setup/teardown.
  - For dispatcher tests: use Anvil account #0 private key (`0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80` — Hardhat/Anvil deterministic account). Broadcast to Anvil.
  **Must NOT do**: Mock Redis or PostgreSQL (use real Docker services). Mock web3 (use real Anvil). Depend on MegaETH testnet.
  **Recommended Agent Profile**: `sisyphus-junior` — integration tests with real services, follows Phase 1 test conventions.
  **Parallelization**: Wave 5, no blockers (can run in parallel with T11).
  **References**: `tests/integration/test_pipeline.py` (fixture pattern, stream isolation, DB truncation), `tests/integration/test_state.py` (repository integration tests), `tests/integration/test_streams.py` (consumer/stream integration tests), `tests/conftest.py` (redis_client, db_session, mini_block_factory fixtures), `app/agents/dispatcher.py`, `app/state/balance_repository.py`, `app/state/balance_worker.py`
  **Acceptance Criteria**: All 20-24 integration tests pass when run with `SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_dispatcher.py tests/integration/test_asset_balances.py tests/integration/test_balance_worker.py -v`
  **QA Scenarios**:
  1. **All integration tests pass**: Tool: `Bash (uv run pytest)`. Prerequisites: Docker services running, Anvil running. Steps: `SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_dispatcher.py tests/integration/test_asset_balances.py tests/integration/test_balance_worker.py -v`. Expected: all tests green, 0 failures. Evidence: `.sisyphus/evidence/task-12-integration-tests.txt`

## Final Verification Wave
F1. **Plan Compliance Audit** (`oracle`): Verify all 12 tasks addressed, guardrails enforced, deliverables match plan. Check: STREAM pattern used (not LIST), subgraph client deferred, slippage cap enforced at schema level, address normalization consistent, single-worker dispatch constraint enforced, balance worker integrated into supervisor.
F2. **Code Quality Review** (`unspecified-high`): ruff clean, mypy clean, no bare `except:`, all type hints present, docstrings on public functions, `from __future__ import annotations` in all new files.
F3. **Integration Smoke Test** (`unspecified-high` + `playwright` if UI, N/A here): Run full test suite including Phase 1 tests: `uv run -m pytest tests/ --ignore=tests/soak --ignore=tests/integration/test_chaos.py -v`. Verify no regressions in existing 50 tests. Run Phase 2 integration tests: `uv run -m pytest tests/integration/test_dispatcher.py tests/integration/test_asset_balances.py tests/integration/test_balance_worker.py -v`.
F4. **Scope Fidelity Check** (`deep`): Verify no LangGraph agent code exists. Verify no LLM imports. Verify no subgraph client code. Verify dead-letter stream for execution queue. Verify balance worker creates its own consumer group on the main stream (no new stream). Verify metric counters exported in `/metrics`. Verify worker_count defaults to 1.

## Commit Strategy
Single commit after all waves + final verification pass:
```
Phase 2: cognitive infrastructure layer (intent schemas, asset state, dispatcher worker pool)

Infrastructure layer bridging LangGraph cognition (Phase 3) to MegaETH execution:
- app/schemas/intent.py: ExecutionPayload + TriggerCondition Pydantic schemas
  with 1% slippage cap enforcement
- app/state/models.py: AssetBalance + TokenTransfer SQLModel tables (composite PKs)
- app/agents/dispatch.py: ExecutionQueue with Redis STREAM + consumer groups
  (XREADGROUP/XACK/XAUTOCLAIM/dead-letter, matching Phase 1 patterns)
- app/state/transfer_parser.py: ERC-20 Transfer event extractor from receipt logs
- app/state/balance_repository.py: AssetBalanceRepository with upsert/query
- app/state/balance_worker.py: BalanceTrackerWorker consuming from Phase 1 stream
- app/agents/dispatcher.py: TransactionExecutor (sign + broadcast via
  eth_sendRawTransactionSync) + WorkerPool (async workers consuming from stream)
- app/api/dependencies.py: Phase 2 FastAPI dependencies + health check extension
- app/api/main.py supervisor: BalanceTrackerWorker + WorkerPool integration
- Config: RPC_URL, PRIVATE_KEY, MEGAETH_HISTORICAL_GRAPHQL_URL env vars
- Metrics: 10 new Prometheus counters (dispatcher, transfer parser, balances)
- Tests: ~40 tests (unit: schemas + parser; integration: dispatcher + balances + worker)
```

## Success Criteria
- [ ] `ExecutionPayload` schema enforces max 1% slippage at Pydantic level
- [ ] ExecutionQueue uses Redis STREAM with consumer groups (consistent with Phase 1)
- [ ] WorkerPool starts/stops cleanly via supervisor; handles SIGTERM gracefully
- [ ] TransactionExecutor signs and broadcasts to local Anvil successfully (uses `eth_sendRawTransaction` on Anvil; `eth_sendRawTransactionSync` reserved for MegaETH testnet/mainnet)
- [ ] BalanceTrackerWorker consumes from Phase 1 stream and upserts asset balances
- [ ] ERC-20 Transfer parser correctly extracts deltas from receipt logs; handles edge cases
- [ ] AssetBalanceRepository upserts idempotently via ON CONFLICT DO UPDATE; negative balances clamped to 0
- [ ] Health check endpoint reports RPC and execution stream status
- [ ] All new Prometheus metrics exported via `/metrics`
- [ ] ruff clean, mypy clean
- [ ] ~40 new tests pass (unit + integration); no regression in Phase 1 tests
- [ ] No LangGraph, LLM, or subgraph client code in Phase 2 deliverable
- [ ] WorkerPool constrained to single worker (worker_count=1) per PRIVATE_KEY
