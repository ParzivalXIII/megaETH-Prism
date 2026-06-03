# Phase 3: LangGraph Multi-Agent Cognition Layer

## TL;DR
> **Quick Summary**: Build the LangGraph cognitive layer that runs as a background polling loop, queries Qdrant memory and PostgreSQL state, produces typed ExecutionPayload intents, and dispatches them to the Phase 2 Redis execution stream. Two-agent architecture: Market Intelligence (semantic search + subgraph) → Portfolio Router (balance evaluation → ExecutionPayload generation).
> **Deliverables**: `app/agents/state.py`, `app/agents/graph.py`, `app/agents/runner.py`, `app/agents/tools/subgraph.py`, `app/agents/tools/local_state.py`, `app/agents/tools/memory.py`, `app/agents/tools/dispatch.py`, `app/agents/system_prompt.py`, config/supervisor/deps/API updates, LLM validation POC, ~25 tests
> **Estimated Effort**: Large
> **Parallel Execution**: YES — 6 waves, 14 tasks
> **Critical Path**: T0 (POC) → T8 (graph) → T9 (runner) → T10 (supervisor)

## Context
### Original Request
Build Phase 3 of the MegaETH event-driven orchestration system — the LangGraph multi-agent cognition layer that bridges the slow-path cognitive intelligence with the fast-path Phase 1/2 infrastructure. Agents execute async tool chains (Qdrant similarity search, PostgreSQL balance queries, Envio subgraph historical data) and output atomic `ExecutionPayload` JSON directly into the `megaeth:stream:executions` Redis stream.

### Interview Summary
- **Scope**: Full integration (Option B) — 4 primary files + config, dependencies, API route, supervisor, system prompt, tests
- **LLM**: OpenCode Go endpoint (`https://opencode.ai/zen/go/v1/chat/completions`), model `deepseek-v4-flash`, OpenAI-compatible via `langchain-openai.ChatOpenAI`
- **Invocation**: Background polling loop in supervisor (primary) + `POST /agent/invoke` endpoint for testing (secondary)
- **Tools**: Fixed (hardcoded) GraphQL queries in subgraph tool — safe, testable, predictable
- **Local dev**: Public Uniswap V3 subgraph as schema placeholder; production uses Envio HyperIndex
- **Safety**: Calldata function-selector allowlist enforced in the dispatch tool

### Metis Review
**Critical findings incorporated:**
1. **LLM validation POC**: Task T0 validates `with_structured_output()` on the OpenCode Go endpoint before any agent code is written. Single Python script proving the endpoint accepts `tools` arrays and returns structured `tool_calls`.
2. **Calldata safety layer**: Task T7 includes a function-selector allowlist in the dispatch tool. Known dangerous selectors (`approve(address,uint256)`, `transferFrom(address,address,uint256)`) blocked; unknown selectors require explicit opt-in.
3. **System prompt**: Task T3 defines the complete system prompt as a module-level constant — agents are instructed on tool usage, output format, safety boundaries, and no-action paths.
4. **Subgraph schema abstraction**: Task T4 uses a configuration-based query selector (`SUBGRAPH_TYPE=uniswap_v3|envio_hyperindex`) to bridge Uniswap V3 dev schema vs. Envio production schema.
5. **Qdrant sync blocking**: Task T6 wraps `SyncQdrantClient.search()` in `asyncio.to_thread()` to avoid blocking the async event loop.
6. **Graph topology**: Task T8 defines a proper graph with 2 conditional edges (opportunity found → portfolio; nothing found → END) and error recovery paths.
7. **Polling loop**: Task T9 specifies interval-based polling (`AGENT_POLL_INTERVAL_SEC`), single invocation at a time, idle backoff.
8. **TriggerCondition evaluators**: Task T13 implements the `price_threshold` and `time_bound` condition evaluators in `TransactionExecutor` that were deferred from Phase 2.

## Work Objectives
### Core Objective
Build a LangGraph StateGraph with two specialist agents (Market Intelligence, Portfolio Router) that autonomously monitors MegaETH state via tool calls, generates typed `ExecutionPayload` intents, and dispatches them to the Phase 2 execution stream. The graph compiles as a singleton and runs as a background task in the supervisor.

### Concrete Deliverables
1. **`app/agents/state.py`**: LangGraph TypedDict state schemas (messages with operator.add reducer, analysis_result, execution_payload, error)
2. **`app/agents/system_prompt.py`**: Module-level system prompt constant defining agent task, tool usage, safety, output format
3. **`app/agents/tools/subgraph.py`**: Async GraphQL client (gql[aiohttp]) with fixed queries, schema abstraction for Uniswap V3 dev / Envio production
4. **`app/agents/tools/local_state.py`**: Portfolio/balance query tool wrapping AssetBalanceRepository
5. **`app/agents/tools/memory.py`**: Cognitive memory tool wrapping CohereEmbedder + QdrantManager (sync wrapped in to_thread)
6. **`app/agents/tools/dispatch.py`**: ExecutionPayload enqueue tool with function-selector allowlist calldata safety
7. **`app/agents/graph.py`**: Compiled LangGraph StateGraph with market_agent + portfolio_agent nodes, conditional routing, error recovery
8. **`app/agents/runner.py`**: Background polling loop with configurable interval, idle backoff, single-invocation lock
9. **`app/api/dependencies.py`**: Replace `get_agent_graph()` stub with compiled graph singleton
10. **Supervisor integration**: Agent runner as 7th concurrent task in `app/api/main.py`
11. **FastAPI route**: `POST /agent/invoke` for testing/debugging (authenticated or local-only)
12. **Config + metrics**: LLM settings, subgraph type, polling interval, 6 new Prometheus counters
13. **Tests**: ~25 tests (unit: tools, calldata safety, state; integration: graph execution, end-to-end pipeline)

### Definition of Done
- [ ] LLM endpoint validated — `with_structured_output()` returns valid `ExecutionPayload` from `deepseek-v4-flash`
- [ ] Graph compiles and runs without crash (even if LLM unavailable — graceful degradation)
- [ ] Market Intelligence Agent successfully queries Qdrant and subgraph via tool calls
- [ ] Portfolio Router Agent generates valid `ExecutionPayload` meeting all Pydantic constraints
- [ ] Dispatch tool enforces function-selector allowlist (rejects dangerous calldata)
- [ ] Background polling loop runs at configured interval, handles idle gracefully
- [ ] `POST /agent/invoke` returns agent state (for debugging)
- [ ] ruff clean, mypy clean
- [ ] ~25 tests pass; no regression in Phase 1 (50) + Phase 2 (47) tests

### Must Have
- LLM validation proof-of-concept before any agent code
- Calldata function-selector allowlist in dispatch tool
- System prompt as a module-level constant (not inline in graph code)
- Subgraph type configuration switch (Uniswap V3 dev / Envio production)
- Qdrant sync calls wrapped in `asyncio.to_thread()`
- Graph conditional routing: opportunity found → portfolio; nothing found → END; error → graceful exit
- Background polling with configurable interval, single invocation, idle backoff
- `price_threshold` and `time_bound` TriggerCondition evaluators (deferred from Phase 2)
- 6 new Prometheus counters (agent_invocations_total, agent_errors_total, agent_payloads_generated_total, tool_call_*, agent_loop_duration_seconds)

### Must NOT Have (Guardrails)
- **MUST NOT** implement generic GraphQL query execution (fixed queries only)
- **MUST NOT** call web3.py from ANY agent code (dispatch only through ExecutionQueue.enqueue)
- **MUST NOT** generate calldata for function selectors NOT in the allowlist
- **MUST NOT** hardcode system prompts inline in graph.py
- **MUST NOT** use synchronous Qdrant calls that block the async event loop
- **MUST NOT** run multiple concurrent graph invocations (single invocation lock)
- **MUST NOT** generate transactions without passing calldata safety validation
- **MUST NOT** create new Redis keys without `megaeth:` prefix
- **MUST NOT** implement multi-chain subgraph support (single endpoint, single chain_id)

## Verification Strategy
**Test approach**: Tests-after, LLM responses mocked.
- **Unit tests**: Tool functions (isolated with mock LLM/subgraph/Qdrant responses), calldata safety (valid/invalid/dangerous selectors), state schema validation, graph node unit tests
- **Integration tests**: Graph execution end-to-end (mock LLM → tool calls → dispatch), agent runner lifecycle (start → poll → stop), API endpoint smoke test
- **Failure path tests**: LLM unavailable, subgraph timeout, Qdrant degraded, calldata blocked, empty state (no balances → no action)
- **No mocking of Redis/PostgreSQL** — use real Docker services
- **LLM responses mocked** — inject predetermined tool_calls via `pytest` monkeypatched `llm.ainvoke()` to avoid API costs

All verification is **agent-executed** — zero manual QA intervention.

## Execution Strategy
### Parallel Execution Waves

```
T0 (POC) ──► Wave 1 ──► Wave 2 ──► Wave 3 ──► Wave 4 ──► Wave 5

             T1 (config)    T4 (subgraph)    T8 (graph)    T9 (runner)   T11 (unit tests)
             T2 (state)     T5 (local)                     T10 (deps+api T12 (int tests)
             T3 (prompt)    T6 (memory)                       +supv)     T13 (triggers)
                            T7 (dispatch)
```

### Dependency Matrix

| Task | Blocks | Blocked By | Wave |
|------|--------|------------|------|
| T0 | T1-T8 | — | 0 |
| T1 | T4, T8 | T0 | 1 |
| T2 | T8 | T0 | 1 |
| T3 | T8 | T0 | 1 |
| T4 | T8 | T1 | 2 |
| T5 | T8 | T2 | 2 |
| T6 | T8 | T2 | 2 |
| T7 | T8, T11 | T0 | 2 |
| T8 | T9, T11 | T1-T7 | 3 |
| T9 | T10 | T8 | 4 |
| T10 | T12 | T9 | 4 |
| T11 | — | T7, T8 | 5 |
| T12 | — | T9, T10 | 5 |
| T13 | — | T8 (requires graph to test triggers) | 5 |

### Agent Dispatch Summary
- **Wave 0** (1 task): LLM validation POC. `sisyphus-junior` — single script, no dependencies.
- **Wave 1** (3 tasks): All parallel — config, state schemas, system prompt. `sisyphus-junior`.
- **Wave 2** (4 tasks): All parallel — tools. Each touches 1 file. `sisyphus-junior`.
- **Wave 3** (1 task): Graph compilation. `sisyphus-junior` with `unspecified-high` rigor.
- **Wave 4** (2 tasks): Runner + integration. `sisyphus-junior`.
- **Wave 5** (3 tasks): Tests (parallel) + trigger evaluators. `sisyphus-junior`.

## TODOs

- [ ] 0. LLM Validation POC — Verify OpenCode Go Structured Output
  **What to do**: Before ANY agent code is written, run a proof-of-concept Python script that validates the OpenCode Go endpoint supports structured output with `deepseek-v4-flash`. This is a **hard blocker** — if structured output fails, the entire ExecutionPayload auto-generation strategy must be redesigned.
  - Create a temporary `scripts/validate_llm.py` that:
    1. Uses `langchain_openai.ChatOpenAI(openai_api_base="https://opencode.ai/zen/go/v1/chat/completions", model="deepseek-v4-flash", api_key=os.environ["OPENCODE_GO_API_KEY"])`
    2. Binds a dummy tool (e.g., `@tool def get_weather(city: str) -> str: ...`)
    3. Calls `llm.invoke([HumanMessage(content="What is the weather in London?")])`
    4. Asserts the response contains `tool_calls`
    5. Tests `llm.with_structured_output(ExecutionPayload)` with a simple prompt: "Generate an ExecutionPayload for calling contract 0x402085c248EeA27D92E8b30b2C58ed07f9E20001 with calldata 0xa9059cbb"
    6. Asserts the returned object is a valid `ExecutionPayload` (passes model_validate)
  - Documents findings: does the endpoint support `tools`? Does `with_structured_output()` work? Are there rate limits or token constraints?
  - If POC fails: plan is paused, user must select alternative LLM or use manual JSON parsing
  - If POC succeeds: evidence stored at `.sisyphus/evidence/task-0-llm-poc.txt`
  **Must NOT do**: Write any agent code. Import LangGraph. Create files outside `scripts/validate_llm.py`.
  **Recommended Agent Profile**: `sisyphus-junior` — single script, well-defined pass/fail.
  **Parallelization**: Wave 0, blocks T1-T8.
  **References**: `app/schemas/intent.py` (ExecutionPayload), `app/core/config.py` (settings pattern for API key), `pyproject.toml` (langchain-openai>=1.2.2 already installed)
  **Acceptance Criteria**: Script runs without error and prints "PASS" or "FAIL" with detailed diagnostics.
  **QA Scenarios**:
  1. **Structured output test**: Tool: `Bash (uv run python)`. Steps: `OPENCODE_GO_API_KEY=opencode-go-<key> uv run python scripts/validate_llm.py`. Expected: "PASS" printed, exit code 0, evidence file created. Evidence: `.sisyphus/evidence/task-0-llm-poc.txt`

- [ ] 1. Config — LLM & Agent Settings
  **What to do**: Extend `app/core/config.py` and `app/core/metrics.py` for Phase 3.
  - **New Settings**: `opencode_go_api_key` (`str`, default `""` — validated by LLM module), `llm_model` (`str`, default `"deepseek-v4-flash"`), `llm_base_url` (`str`, default `"https://opencode.ai/zen/go/v1/chat/completions"`), `llm_temperature` (`float`, default `0.0` — deterministic for consistent outputs), `llm_max_tokens` (`int`, default `4096`), `subgraph_type` (`Literal["uniswap_v3", "envio_hyperindex"]`, default `"uniswap_v3"` — controls query selection), `agent_poll_interval_sec` (`int`, default `60` — seconds between graph invocations), `agent_poll_idle_backoff_sec` (`int`, default `300` — seconds after no-action), `agent_max_tool_calls` (`int`, default `10` — recursion guard), `agent_calldata_allowlist` (`list[str]`, default `["0xa9059cbb","0x095ea7b3","0x38ed1739"]` — hex selectors for transfer/approve/swapExactTokensForTokens), `agent_system_prompt` (`str`, default loads from system_prompt module — see T3)
  - **New Metrics**: `agent_invocations_total`, `agent_invocations_success_total`, `agent_invocations_error_total`, `agent_payloads_generated_total`, `agent_tool_call_total` (counter for all tool calls), `agent_loop_duration_seconds` (gauge for last invocation duration) — all module-level `int`/`float`, exported in `__all__` for `/metrics`
  - **Update `.env`**: Add `OPENCODE_GO_API_KEY=`, `LLM_MODEL=deepseek-v4-flash`, `SUBGRAPH_TYPE=uniswap_v3`
  - **Update `.env.example`**: Same additions with comments
  - Validate: `opencode_go_api_key` is non-empty when agent is initialized (fail fast), `subgraph_type` is in allowed set, `agent_poll_interval_sec >= 10`
  **Must NOT do**: Add WebSocket config for LLM. Add model selection logic beyond the single model.
  **Recommended Agent Profile**: `sisyphus-junior` — config extension, metric counter addition.
  **Parallelization**: Wave 1, no blockers, blocks T4 + T8.
  **References**: `app/core/config.py` (Settings class), `app/core/metrics.py` (counter naming), `.env`, `.env.example`
  **Acceptance Criteria**: Settings resolve from `.env`, all metrics importable and at 0, `.env.example` updated.
  **QA Scenarios**:
  1. **Config resolution**: Tool: `Bash (uv run python)`. Steps: `from app.core.config import settings; print(settings.llm_model, settings.subgraph_type, settings.agent_poll_interval_sec)`. Expected: defaults printed. Evidence: `.sisyphus/evidence/task-1-config.txt`

- [ ] 2. `app/agents/state.py` — LangGraph TypedDict State & Context Schemas
  **What to do**: Create `app/agents/state.py` with typed state schemas for the LangGraph graph.
  - `AgentState` TypedDict:
    - `messages`: `Annotated[list[BaseMessage], operator.add]` — accumulated messages (tool calls, LLM responses)
    - `analysis_result`: `dict[str, Any] | None` — Market Intelligence agent output (anomalies, opportunities, market context)
    - `execution_payload`: `ExecutionPayload | None` — the final payload to dispatch (None = no action)
    - `user_address`: `str` — target user address for portfolio queries (default "")
    - `error`: `str | None` — last error message (None = no error)
    - `current_phase`: `str` — tracks graph position for debugging ("market_analysis" | "portfolio_routing" | "dispatching" | "done")
  - `AgentContext` TypedDict (runtime context, immutable per invocation):
    - `db_session_factory`: `async_sessionmaker[AsyncSession]`
    - `redis_client`: Any (redis.asyncio.Redis)
    - `qdrant_client`: Any (QdrantManager)
    - `embedder`: Any (CohereEmbedder)
    - `execution_queue`: Any (ExecutionQueue)
    - `subgraph_client`: Any (SubgraphClient from T4)
    - `llm`: Any (ChatOpenAI instance)
  - Use `from __future__ import annotations` and complete type hints.
  **Must NOT do**: Define graph nodes (graph.py). Define tools (tools/*.py). Import LangGraph graph builder.
  **Recommended Agent Profile**: `sisyphus-junior` — TypedDict definitions, follows langgraph-orchestrator pattern.
  **Parallelization**: Wave 1, no blockers, blocks T8.
  **References**: `app/schemas/intent.py` (ExecutionPayload), langgraph-orchestrator skill examples (AgentState/AgentContext pattern), `app/agents/dispatch.py` (ExecutionQueue)
  **Acceptance Criteria**: Both TypedDicts importable, fields have correct type annotations, `messages` uses `Annotated` with `operator.add` reducer.
  **QA Scenarios**:
  1. **Schema importable**: Tool: `Bash (uv run python)`. Steps: `from app.agents.state import AgentState, AgentContext; print(dict(AgentState.__annotations__).keys(), dict(AgentContext.__annotations__).keys())`. Expected: field names printed. Evidence: `.sisyphus/evidence/task-2-state.txt`

- [ ] 3. `app/agents/system_prompt.py` — Agent System Prompt
  **What to do**: Create `app/agents/system_prompt.py` containing the complete system prompt as a module-level constant. This is the **specification** for agent behavior — it defines what agents look for, how they use tools, and how they produce output.
  - `AGENT_SYSTEM_PROMPT` constant (multi-line string):
    - **Role**: "You are a MegaETH portfolio analysis agent. You monitor on-chain state and identify opportunities."
    - **Tools available**: Lists each tool, its purpose, and when to use it (subgraph: historical data, use query_type values "token_price_history"/"recent_swaps"/"pool_stats"; local_state: current balances; memory: semantic context; dispatch: execute actions)
    - **Decision flow**: "First, query cognitive memory for recent market context. Then query historical subgraph data. Evaluate portfolio balances. If you find an actionable opportunity, generate an ExecutionPayload. If nothing actionable exists, respond with 'NO_ACTION'."
    - **Safety rules**: "Never generate calldata for approve() or transferFrom(). Only use known function selectors from the allowlist. Max slippage is 1% (100 bps). Max gas price is 1,000,000 wei."
    - **Output format**: "When you decide to act, call the dispatch_execution tool with a valid ExecutionPayload. The payload must include: target_contract (0x-prefixed), call_data (0x-prefixed hex), trigger_condition (always), max_slippage_bps (≤100), created_by_agent='portfolio-agent'."
    - **No-action**: "If no opportunity exists, do NOT call any tool. Just respond explaining why no action is needed."
  - Also export `AGENT_SYSTEM_PROMPT` from `app/agents/__init__.py`.
  **Must NOT do**: Include implementation code.
  **Recommended Agent Profile**: `sisyphus-junior` — simple string constant, domain knowledge from MegaETH context.
  **Parallelization**: Wave 1, no blockers, blocks T8.
  **References**: AGENTS.md (slippage 1%, gas model), `app/schemas/intent.py` (ExecutionPayload fields), `app/agents/tools/` (tool names and descriptions)
  **Acceptance Criteria**: Prompt constant importable, includes all 4 tool references, mentions 1% slippage cap, defines no-action path.
  **QA Scenarios**:
  1. **Prompt exists**: Tool: `Bash (uv run python)`. Steps: `from app.agents.system_prompt import AGENT_SYSTEM_PROMPT; print(len(AGENT_SYSTEM_PROMPT)); print("dispatch_execution" in AGENT_SYSTEM_PROMPT); print("NO_ACTION" in AGENT_SYSTEM_PROMPT)`. Expected: length > 500, both `True`. Evidence: `.sisyphus/evidence/task-3-prompt.txt`

- [ ] 4. `app/agents/tools/subgraph.py` — Async Subgraph Client with Fixed Queries
  **What to do**: Create `app/agents/tools/subgraph.py` with a `LangGraph @tool` that queries historical on-chain data asynchronously using `gql[aiohttp]`.
  - **Schema abstraction**: Uses `settings.subgraph_type` to select query set:
    - `"uniswap_v3"` → queries against public Uniswap V3 subgraph schema (placeholder for local dev)
    - `"envio_hyperindex"` → queries against Envio HyperIndex schema (production)
  - **Fixed queries** (NOT generic — each is a named function):
    - `SubgraphClient.fetch_token_price_history(token_address: str, days: int = 7) -> list[dict]` — daily OHLCV
    - `SubgraphClient.fetch_recent_swaps(pool_address: str, limit: int = 100) -> list[dict]` — recent swap events
    - `SubgraphClient.fetch_pool_stats(pool_address: str) -> dict` — TVL, volume, fees
  - Each fixed query has a `gql()` pre-compiled query string and returns typed dicts.
  - `SubgraphClient.__init__(url: str)`: Accepts `settings.megaeth_historical_graphql_url` (Uniswap V3 placeholder or Envio URL). Uses `AIOHTTPTransport` + `fetch_schema_from_transport=True`.
  - `async execute(query_str, variables) -> dict`: Internal helper, handle `TransportQueryError`.
  - `async close()`: Close HTTP session.
  - **LangGraph `@tool` wrapper**: `query_historical_subgraph(query_type: str, params: dict[str, Any]) -> dict` — the tool the LLM calls. Maps `query_type` to fixed query function.
  - Use `from __future__ import annotations` and complete type hints.
  **Must NOT do**: Accept generic GraphQL strings from the LLM. Implement `fetch_schema_from_transport` for every call (cached). Mix Uniswap V3 queries with Envio queries.
  **Recommended Agent Profile**: `sisyphus-junior` — gql client, tool decorator, follows existing Phase 1 patterns.
  **Parallelization**: Wave 2, blocks T8, blocked by T1.
  **References**: `gql` library (already in pyproject.toml), `app/core/config.py` (megaeth_historical_graphql_url, subgraph_type), `app/agents/state.py` (AgentContext fields), langgraph-orchestrator skill (tool decorators)
  **Acceptance Criteria**:
  - `SubgraphClient` connects to public Uniswap V3 subgraph without error
  - `fetch_token_price_history("0x...", 7)` returns list of dicts or empty list (no crash on empty data)
  - `query_historical_subgraph` is a valid `@tool` importable by LangGraph
  - Subgraph timeout or 404 returns empty result gracefully (no crash)
  **QA Scenarios**:
  1. **Subgraph client connects**: Tool: `Bash (uv run python)`. Prerequisites: Internet access. Steps: Create SubgraphClient with public Uniswap V3 URL, call `fetch_pool_stats(known_pool_address)`. Expected: returns dict (may be empty if pool not found, but no network error). Evidence: `.sisyphus/evidence/task-4-subgraph.txt`

- [ ] 5. `app/agents/tools/local_state.py` — Portfolio & Balance Query Tool
  **What to do**: Create `app/agents/tools/local_state.py` with `LangGraph @tool` wrapping the AssetBalanceRepository.
  - **`query_portfolio` tool**: `async def query_portfolio(user_address: str, token_symbol: str = "") -> dict` — queries PostgreSQL via `AssetBalanceRepository.get_portfolio()`. Returns structured dict with `user_address`, `balances` (list of `{token_address, token_symbol, balance_raw, balance_human}` matching AssetBalance model fields), `count`. If `token_symbol` is provided, filters balances matching that symbol.
  - **`query_balance` tool**: `async def query_balance(user_address: str, token_address: str) -> dict` — queries single balance via `AssetBalanceRepository.get_balance()`. Returns `{token_address, token_symbol, balance_raw, balance_human}` or `None`.
  - Both tools manage their own `AsyncSession` lifecycle: get factory from runtime context (`config["configurable"]["db_session_factory"]`), create session, query, close session.
  - Address inputs are auto-lowercased (matching repository behavior).
  - Increment `agent_tool_call_total` metric on each invocation.
  - Use `from __future__ import annotations` and complete type hints.
  **Must NOT do**: Accept raw SQL. Expose repository internals. Create sessions without proper async context manager.
  **Recommended Agent Profile**: `sisyphus-junior` — LangGraph tool, repository wrapper, follows existing Phase 2 patterns.
  **Parallelization**: Wave 2, blocks T8, blocked by T2.
  **References**: `app/state/balance_repository.py` (AssetBalanceRepository), `app/state/models.py` (AssetBalance), `app/core/metrics.py` (agent_tool_call_total), `app/api/dependencies.py` (get_db_session_factory pattern)
  **Acceptance Criteria**:
  - `query_portfolio(addr)` returns all balances for user when DB has data
  - `query_balance(addr, token)` returns single balance or None
  - Both tools are valid `@tool` decorators importable by LangGraph
  **QA Scenarios**:
  1. **Portfolio query**: Tool: `Bash (uv run python)`. Prerequisites: PostgreSQL running, AssetBalance table created, seed data inserted. Steps: Call `query_portfolio(known_address)`. Expected: returns dict with balances list. Evidence: `.sisyphus/evidence/task-5-portfolio.txt`

- [ ] 6. `app/agents/tools/memory.py` — Cognitive Memory Search Tool
  **What to do**: Create `app/agents/tools/memory.py` with `LangGraph @tool` wrapping QdrantManager.search() + CohereEmbedder.embed(). **Must wrap sync Qdrant calls in `asyncio.to_thread()`** to avoid blocking the async event loop.
  - **`search_cognitive_memory` tool**: `async def search_cognitive_memory(query: str, limit: int = 10) -> dict` — embeds query text via `CohereEmbedder.embed()`, then searches Qdrant via `QdrantManager.search()` wrapped in `asyncio.to_thread()`, returns top-K results with scores and summaries.
  - Returns: `{query, results: [{id, score, summary, block_number, metadata}, ...], count}`
  - Embedder and Qdrant client sourced from runtime context (`config["configurable"]["embedder"]`, `config["configurable"]["qdrant_client"]`).
  - Increment `agent_tool_call_total` metric. Handle degraded mode: if embedder unavailable, return `{error: "embedding service unavailable"}`.
  - Use `from __future__ import annotations` and complete type hints.
  **Must NOT do**: Call Qdrant synchronously (event loop blocking). Cache embeddings across invocations (let embedder manage its own cache). Add new collection creation.
  **Recommended Agent Profile**: `sisyphus-junior` — LangGraph tool, wraps existing Phase 1 services.
  **Parallelization**: Wave 2, blocks T8, blocked by T2.
  **References**: `app/vector/qdrant_client.py` (QdrantManager, SyncQdrantClient), `app/vector/embedder.py` (CohereEmbedder), `app/core/metrics.py` (agent_tool_call_total), `app/agents/state.py` (AgentContext)
  **Acceptance Criteria**:
  - `search_cognitive_memory("USDC price")` returns results when cohere-embed and Qdrant are healthy
  - Returns `error` dict when embedder is unavailable (no crash)
  - Search uses `asyncio.to_thread()` wrapper (verify no event loop warnings)
  **QA Scenarios**:
  1. **Memory search**: Tool: `Bash (uv run python)`. Prerequisites: Docker cohere-embed running, Qdrant with data. Steps: Call `search_cognitive_memory("recent swaps")`. Expected: returns dict with results key, score values between 0-1. Evidence: `.sisyphus/evidence/task-6-memory.txt`

- [ ] 7. `app/agents/tools/dispatch.py` — ExecutionPayload Dispatch Tool with Calldata Safety
  **What to do**: Create `app/agents/tools/dispatch.py` with `LangGraph @tool` that validates and enqueues ExecutionPayload to the Phase 2 execution stream. This is the **sole path** from cognition to execution — all safety checks happen here.
  - **`dispatch_execution` tool**: `async def dispatch_execution(payload: dict[str, Any]) -> dict` — validates a dict against ExecutionPayload schema, applies calldata safety checks, enqueues via `ExecutionQueue.enqueue()`, returns `{execution_id: str, status: "queued"}`.
  - **Calldata safety layer**: Extract function selector (first 10 chars of calldata, after 0x — hex prefix). Compare against `settings.agent_calldata_allowlist` (list of hex selectors):
    - Known safe selectors in allowlist (e.g., `0xa9059cbb` = transfer, `0x095ea7b3` = approve, `0x38ed1739` = swapExactTokensForTokens): ALLOWED
    - Known dangerous selectors hardcoded in blocklist (`0x23b872dd` = transferFrom, `0x42842e0e` = safeTransferFrom): BLOCKED — return error regardless of allowlist
    - Unknown selectors: BLOCKED by default — return error with "unknown selector: {selector}"
    - All blocked calls log WARNING and return `{status: "rejected", reason: "calldata blocked: {reason}"}`
    - Admin bypass: setting `agent_calldata_allowlist` to `["*"]` allows all (for testing only, documented as unsafe)
  - Enforces `max_slippage_bps ≤ 100` at the tool level (defense-in-depth, even after Pydantic validation).
  - Sets `created_by_agent="portfolio-agent"` if not already set.
  - Sources `execution_queue` from runtime context.
  - Increment `agent_payloads_generated_total` metric on successful enqueue.
  - Use `from __future__ import annotations` and complete type hints.
  **Must NOT do**: Call web3.py. Bypass calldata validation. Allow unknown selectors without explicit admin action. Accept raw hex strings without 0x prefix.
  **Recommended Agent Profile**: `sisyphus-junior` with `unspecified-high` rigor — this is the safety boundary between LLM and execution.
  **Parallelization**: Wave 2, blocks T8 + T11, blocked by T0 (needs validated ExecutionPayload schema).
  **References**: `app/schemas/intent.py` (ExecutionPayload, field_validator), `app/agents/dispatch.py` (ExecutionQueue.enqueue), `app/core/config.py` (agent_calldata_allowlist), `app/core/metrics.py` (agent_payloads_generated_total), AGENTS.md (slippage 1%, no web3.py in agents)
  **Acceptance Criteria**:
  - `dispatch_execution(valid_payload_dict)` enqueues and returns execution_id
  - `dispatch_execution({calldata: "0x23b872dd..."})` (transferFrom selector) → rejected with "calldata blocked"
  - `dispatch_execution({calldata: "0xdeadbeef..."})` (unknown selector) → rejected
  - `dispatch_execution({calldata: "0xa9059cbb..."})` (transfer selector, allowed) → queued
  - `dispatch_execution({max_slippage_bps: 101})` → rejected
  **QA Scenarios**:
  1. **Safe calldata accepted**: Tool: `Bash (uv run python)`. Steps: Call dispatch_execution with known-safe transfer selector + valid contract address. Expected: returns `{status: "queued", execution_id: "..."}`. Evidence: `.sisyphus/evidence/task-7-safe-call.txt`
  2. **Dangerous calldata blocked**: Tool: `Bash (uv run python)`. Steps: Call dispatch_execution with transferFrom selector `0x23b872dd`. Expected: returns `{status: "rejected", reason: "calldata blocked: ..."}`. Evidence: `.sisyphus/evidence/task-7-blocked-call.txt`

- [ ] 8. `app/agents/graph.py` — Multi-Agent LangGraph StateGraph
  **What to do**: Create `app/agents/graph.py` with `build_agent_graph()` factory that compiles a LangGraph StateGraph with two specialist agent nodes and conditional routing. This is the **brain** of Phase 3.
  - **Graph topology** (with all conditional edges):
    ```
    START → market_intelligence_node
                │
        ┌───────┴────────┐
        │ (ran tools)     │ (no tools ran / error)
        ▼                 ▼
    portfolio_router_node  END (no action)
        │
    ┌───┴───────────┐
    │ (got payload)  │ (no payload / error)
    ▼                ▼
    END              END (no action)
    ```
    Dispatch happens as a **tool call** within `portfolio_router_node` — when the LLM calls `dispatch_execution`, the tool enqueues to Redis AND sets `state["execution_payload"]` as a side effect. No separate dispatch node needed.
  - **`market_intelligence_node`**: LLM node. Binds tools `[search_cognitive_memory, query_historical_subgraph]`. System prompt from T3. Wrapped with `llm.bind_tools(...)`. Returns AIMessage (may include tool_calls). Uses LangGraph `ToolNode` for tool execution. Conditional edge: `_has_tool_calls(state) → "portfolio_router_node"` if tools were used and no error, else `END`.
  - **`portfolio_router_node`**: LLM node. Binds tools `[query_portfolio, query_balance, search_cognitive_memory, dispatch_execution]`. Receives context from market_intelligence analysis. Generates ExecutionPayload via `dispatch_execution` tool call OR returns "NO_ACTION". The `dispatch_execution` tool sets `state["execution_payload"]` as a side effect after successful enqueue. Conditional edge: `_has_payload(state) → END` always — dispatch already happened, just route to termination with state inspection for debugging.
  - **`_has_tool_calls` routing function**: Checks state for tool calls in last message. Returns `"portfolio_router_node"` if tools were invoked, `END` if no tools (LLM had nothing to analyze).
  - **`_has_payload` routing function**: Checks `state["execution_payload"]` for non-None value. This field is set by the `dispatch_execution` tool as a side effect (the tool writes `{"execution_payload": payload.model_dump()}` to state via LangGraph's ToolNode mechanism). Returns `END` either way — the graph terminates here since dispatch already happened.
  - **`build_agent_graph(llm, tools, system_prompt, checkpointer=None) -> CompiledStateGraph`**: Factory accepting dependencies. Uses `MemorySaver()` as default checkpointer. Returns compiled graph ready for `.ainvoke()`.
  - Error handling: each node wraps in try/except; on error, sets `state["error"]`, logs ERROR, routes to END gracefully (no crash).
  - Recursion guard: `agent_max_tool_calls` limits the number of tool calls per invocation (configurable, default 10). Exceeding limit → end with warning.
  - Use `from __future__ import annotations` and complete type hints.
  - Export `build_agent_graph` from `app/agents/__init__.py`.
  **Must NOT do**: Hardcode system prompt inline. Hardcode tool definitions (accept them as parameters). Call web3.py. Generate calldata (LLM generates string, dispatch tool validates).
  **Recommended Agent Profile**: `sisyphus-junior` with `unspecified-high` rigor — this is the core of Phase 3, must follow langgraph-orchestrator patterns strictly.
  **Parallelization**: Wave 3, blocks T9 + T11, blocked by T1-T7.
  **References**: langgraph-orchestrator skill (StateGraph, ToolNode, conditional_edges), `app/agents/state.py` (AgentState, AgentContext), `app/agents/system_prompt.py` (AGENT_SYSTEM_PROMPT), `app/agents/tools/` (all tool modules), `app/schemas/intent.py` (ExecutionPayload), langgraph docs: `with_structured_output`, `bind_tools`, `tools_condition`
  **Acceptance Criteria**:
  - `build_agent_graph(llm, tools, prompt)` returns compiled StateGraph that supports `.ainvoke()`
  - Graph runs end-to-end with mock LLM (inject predetermined tool_calls)
  - Market intelligence node receives system prompt and has access to memory + subgraph tools
  - Portfolio router node receives analysis context and has access to portfolio + dispatch tools
  - Conditional routing: no tool calls → END; got payload → dispatch; no payload/wrong answer → END with no error
  - Error in any node → sets error state, logs, exits gracefully
  **QA Scenarios**:
  1. **Graph compiles**: Tool: `Bash (uv run python)`. Steps: `from app.agents.graph import build_agent_graph; from app.agents.tools.local_state import query_portfolio; g = build_agent_graph(mock_llm, [query_portfolio], "test prompt"); print(g.get_graph().draw_ascii())`. Expected: ASCII graph printed, no import errors. Evidence: `.sisyphus/evidence/task-8-graph-compiles.txt`

- [ ] 9. `app/agents/runner.py` — Background Polling Loop
  **What to do**: Create `app/agents/runner.py` with `AgentRunner` class that runs the compiled graph as a background polling loop in the supervisor. Follows the Phase 1/2 worker pattern.
  - **`AgentRunner.__init__(graph: CompiledStateGraph, context: AgentContext, interval_sec: int, idle_backoff_sec: int)`**:
    - `graph`: Compiled LangGraph graph from T8
    - `context`: AgentContext with all runtime dependencies
    - `interval_sec`: Normal polling interval (default 60s from settings)
    - `idle_backoff_sec`: Interval after no-action (default 300s from settings) — avoids spamming LLM when nothing to do
    - `_shutdown_event`: `asyncio.Event` for graceful shutdown (matching Phase 1/2 pattern)
    - `_invocation_lock`: `asyncio.Lock` — ensures only one graph invocation at a time
  - **`start()`**: Create asyncio task running `_poll_loop()`. Log startup.
  - **`stop()`**: Set shutdown event. Wait for task with 30s timeout. Log stop.
  - **`shutdown_event` property**: Public access for supervisor.
  - **`_poll_loop()`**: Main loop:
    1. Wait for `interval_sec` (or `idle_backoff_sec` if last run produced no action)
    2. Acquire `_invocation_lock`
    3. Invoke graph: `result = await self.graph.ainvoke({"messages": [HumanMessage(content="Analyze current MegaETH market state and portfolio.")], "user_address": self.default_user}, config={"configurable": self.context}, context=self.context)`
    4. Check for error in result state
    5. If `execution_payload` is not None: set next wait to `interval_sec` (normal)
    6. If `execution_payload` is None: set next wait to `idle_backoff_sec` (idle)
    7. Increment metrics
    8. Check shutdown event before next iteration
  - **`default_user`**: Configurable target address to monitor (from settings).
  - Use `from __future__ import annotations` and complete type hints.
  **Must NOT do**: Run concurrent invocations. Poll faster than 10s. Call LLM directly.
  **Recommended Agent Profile**: `sisyphus-junior` — follows Phase 1/2 worker patterns (event, lock, poll loop).
  **Parallelization**: Wave 4, blocks T10, blocked by T8.
  **References**: `app/agents/dispatcher.py` (WorkerPool lifecycle), `app/state/balance_worker.py` (BalanceTrackerWorker lifecycle), `app/agents/state.py` (AgentContext), `app/agents/graph.py` (build_agent_graph), `app/core/config.py` (agent_poll_interval_sec, agent_poll_idle_backoff_sec)
  **Acceptance Criteria**:
  - `AgentRunner.start()` spawns background task
  - `AgentRunner.stop()` triggers clean shutdown within 30s
  - `shutdown_event` is publicly accessible
  - Poll loop respects idle backoff after no-action invocation
  - Single invocation lock prevents concurrent runs
  **QA Scenarios**:
  1. **Runner lifecycle**: Tool: `Bash (uv run python)`. Steps: Create AgentRunner with mock graph that returns no payload. Start, wait 2s, stop. Expected: "agent runner stopped" in logs, no crash. Evidence: `.sisyphus/evidence/task-9-runner-lifecycle.txt`

- [ ] 10. Dependencies, API Route & Supervisor Integration
  **What to do**: Wire the compiled agent graph into the existing Phase 1/2 infrastructure.
  - **`app/api/dependencies.py`**: Replace `get_agent_graph()` returning None with a factory that calls `build_agent_graph()` from T8, passing `ChatOpenAI` instance, all tools from T4-T7, and `AGENT_SYSTEM_PROMPT` from T3. Cache as module-level singleton. On first call: validate `opencode_go_api_key` is set (fail fast with clear error). Register as FastAPI `Depends()`.
  - **`app/api/routes/agent.py`** (new): FastAPI router with `POST /agent/invoke` endpoint. Accepts JSON body `{"user_address": "0x...", "prompt": "Check portfolio status"}`. Gets compiled graph from `Depends(get_agent_graph)`. Invokes graph synchronously (awaits result, returns state dict). Returns `{status: "ok"|"error", execution_payload: {...}|null, analysis: "...", error: "..."}`. No authentication for now (local dev); document as "local-only."
  - **`app/api/main.py`**: Update `supervise()` to create and start `AgentRunner` as 7th concurrent task. After Phase 2 workers, before task list. Create `agent_context` dict from dependencies. Add `agent_shutdown_tracker` to tasks list. Update docstring. On shutdown: stop `AgentRunner` before other workers.
  - **`app/agents/__init__.py`**: Export `build_agent_graph`, `AgentRunner`, `AgentState`, `AgentContext`, `AGENT_SYSTEM_PROMPT`.
  **Must NOT do**: Add authentication middleware (deferred). Expose agent endpoint to production network without firewall. Start runner without validating API key.
  **Recommended Agent Profile**: `sisyphus-junior` — wiring task, follows existing dependency/supervisor patterns.
  **Parallelization**: Wave 4, blocks T12, blocked by T9.
  **References**: `app/api/dependencies.py` (singleton pattern), `app/api/main.py` (supervise function, task list), `app/agents/graph.py` (build_agent_graph), `app/agents/runner.py` (AgentRunner)
  **Acceptance Criteria**:
  - `get_agent_graph()` returns compiled graph (not None)
  - `POST /agent/invoke` with valid JSON returns 200 with agent state
  - Supervisor starts 7 concurrent tasks (api server + ingestion daemon + state tracker + vector indexer + balance tracker + worker pool + agent runner)
  - Agent runner appears in startup log
  - OpenCode Go API key validated on first graph invocation
  **QA Scenarios**:
  1. **Agent endpoint smoke**: Tool: `Bash (curl)`. Prerequisites: Supervisor running. Steps: `curl -X POST http://127.0.0.1:8080/agent/invoke -H "Content-Type: application/json" -d '{"user_address": "0x0000000000000000000000000000000000000000", "prompt": "test"}'`. Expected: 200 with JSON body containing status. Evidence: `.sisyphus/evidence/task-10-agent-endpoint.txt`

- [ ] 11. Unit Tests — Tools, Calldata Safety, State, Graph Nodes
  **What to do**: Create unit tests for all Phase 3 modules. LLM responses are **mocked** — no real API calls.
  - **`tests/unit/test_agent_state.py`** (3 tests): State schema validation, messages accumulation, error propagation.
  - **`tests/unit/test_calldata_safety.py`** (6 tests):
    - `test_safe_transfer_accepted` — known transfer selector → allowed
    - `test_safe_approve_blocked` — approve selector excluded from allowlist → blocked
    - `test_dangerous_transferfrom_blocked` — transferFrom → blocked
    - `test_unknown_selector_blocked` — `0xdeadbeef` → blocked
    - `test_wildcard_allowlist_bypasses` — allowlist=["*"] → all accepted
    - `test_slippage_cap_enforced` — 101 bps → rejected
  - **`tests/unit/test_agent_tools.py`** (6 tests): Tool decorator validation, tool signature matching, tool returns correct dict shape, tool error handling (subgraph timeout, DB down, embedder unavailable).
  - **`tests/unit/test_graph_nodes.py`** (6 tests): Market intelligence node with mock LLM (predetermined tool_calls), portfolio router with mock LLM (predetermined payload), no-action path, error path, routing function edge cases, recursion guard.
  - Mock LLM: Create `MockLLM` class that returns predetermined `AIMessage` objects with `tool_calls` or text content. Use `unittest.mock.patch` or dependency injection.
  - Mock subgraph: Return predetermined dicts. Mock Qdrant: Return predetermined ScoredPoint list. Mock embedder: Return predetermined 1024-dim vector.
  **Must NOT do**: Call real LLM API. Use real Docker services (unit tests should mock). Test Redis/Postgres (integration tests).
  **Recommended Agent Profile**: `sisyphus-junior` — standard pytest with mocking.
  **Parallelization**: Wave 5, no blockers (can run in parallel with T12, T13).
  **References**: `tests/unit/test_schemas.py` (test patterns), `tests/unit/test_summarizer.py` (pure function test patterns), `app/agents/graph.py` (graph nodes), `app/agents/tools/dispatch.py` (calldata safety), `app/agents/state.py` (schemas)
  **Acceptance Criteria**: All 21 unit tests pass, no real LLM API calls made.
  **QA Scenarios**:
  1. **All unit tests pass**: Tool: `Bash (uv run pytest)`. Steps: `uv run -m pytest tests/unit/test_agent_state.py tests/unit/test_calldata_safety.py tests/unit/test_agent_tools.py tests/unit/test_graph_nodes.py -v`. Expected: all 21 green. Evidence: `.sisyphus/evidence/task-11-unit-tests.txt`

- [ ] 12. Integration Tests — Graph Execution, Runner Lifecycle, End-to-End Pipeline
  **What to do**: Create integration tests that exercise the full Phase 3 pipeline with real Docker services but mocked LLM.
  - **`tests/integration/test_agent_graph.py`** (6 tests):
    - `test_graph_compiles_and_invokes` — build graph with real tools, invoke with mock LLM producing no-action → returns with messages, no payload
    - `test_graph_produces_payload` — mock LLM calls dispatch_execution with valid payload → execution_payload in state, enqueued to Redis
    - `test_graph_handles_llm_error` — LLM raises exception → graph exits gracefully, error set in state
    - `test_graph_tool_calls_propagate` — mock LLM calls search_cognitive_memory → tool returns results → reflected in state
    - `test_graph_no_action_path` — LLM responds with text only (no tool calls) → END, no payload
    - `test_graph_recursion_guard` — LLM loops tool calls 11 times → exceeds max_tool_calls → END with warning
  - **`tests/integration/test_agent_runner.py`** (4 tests):
    - `test_runner_starts_and_stops` — start runner with mock graph, wait 2s, stop → clean shutdown
    - `test_runner_polling_interval` — verify runner waits at least interval_sec between invocations
    - `test_runner_idle_backoff` — verify runner uses idle_backoff after no-action invocation
    - `test_runner_single_invocation_lock` — verify concurrent start() calls don't spawn duplicate loops
  - **`tests/integration/test_agent_api.py`** (2 tests): POST /agent/invoke returns 200 with valid JSON, POST with missing user_address returns 422.
  - Use Phase 1/2 test patterns: `pytestmark = pytest.mark.asyncio`, dedicated Redis test streams, DB TRUNCATE, `@pytest_asyncio.fixture`. Real Docker for Redis/Postgres/Qdrant. Mocked LLM only.
  **Must NOT do**: Call real LLM API in integration tests. Mock Redis (use real Docker).
  **Recommended Agent Profile**: `sisyphus-junior` — integration tests with mixed real/mock services.
  **Parallelization**: Wave 5, no blockers (can run in parallel with T11, T13).
  **References**: `tests/integration/test_dispatcher.py` (integration patterns), `tests/conftest.py` (fixtures), `app/agents/graph.py`, `app/agents/runner.py`, `app/api/main.py`
  **Acceptance Criteria**: All 12 integration tests pass, no real LLM API calls.
  **QA Scenarios**:
  1. **All integration tests pass**: Tool: `Bash (uv run pytest)`. Prerequisites: Docker services running. Steps: `uv run -m pytest tests/integration/test_agent_graph.py tests/integration/test_agent_runner.py tests/integration/test_agent_api.py -v`. Expected: all 12 green. Evidence: `.sisyphus/evidence/task-12-integration-tests.txt`

- [ ] 13. TriggerCondition Evaluators — Implement `price_threshold` & `time_bound`
  **What to do**: Implement the two TriggerCondition evaluators that were deferred from Phase 2. Updates `app/agents/dispatcher.py`.
  - **`_check_conditions` updates** (in `TransactionExecutor`):
    - `condition_type == "price_threshold"`: Read `params["token_pair"]` (e.g., "ETH/USDC"), `params["threshold"]` (float), `params["direction"]` ("above"/"below"). Call a price feed (stubbed in Phase 3: returns a hardcoded price or reads from Envio subgraph). Compare. Return True/False. Log evaluation result.
    - `condition_type == "time_bound"`: Read `params["execute_at"]` (unix timestamp), `params["before"]` (timestamp). If current time >= execute_at and current time < before → True. Else → False. Log "deferred to {execute_at}".
    - `condition_type == "always"`: unchanged — returns True.
  - **Price feed**: For Phase 3, implement a `_get_price(pair: str) -> float` stub that returns a configurable default (from settings, e.g., `settings.default_eth_price`). Document that production should integrate with an oracle (Chainlink, Pyth, or Envio price feed).
  - Add settings: `default_eth_price` (`float`, default `3000.0`), `default_usdc_price` (`float`, default `1.0`).
  - Add metrics: `trigger_price_evaluated_total`, `trigger_time_evaluated_total`.
  - Add tests in `tests/unit/test_triggers.py`: price above threshold → True, price below threshold → False, time within range → True, time past before → False.
  **Must NOT do**: Integrate a real oracle (stub only for Phase 3). Change the ExecutionPayload or TriggerCondition schema.
  **Recommended Agent Profile**: `sisyphus-junior` — extends existing TransactionExecutor.
  **Parallelization**: Wave 5, no blockers (can run in parallel with T11, T12).
  **References**: `app/agents/dispatcher.py` (_check_conditions method), `app/schemas/intent.py` (TriggerCondition), `app/core/config.py` (new price defaults)
  **Acceptance Criteria**:
  - `price_threshold` with params `{token_pair: "ETH/USDC", threshold: 3000, direction: "above"}` at price 3100 → True
  - `price_threshold` with params `{..., direction: "below"}` at price 3100 → False
  - `time_bound` with `{execute_at: now-10, before: now+10}` → True
  - `time_bound` with `{execute_at: now+10, before: now+20}` → False (not yet)
  **QA Scenarios**:
  1. **Price trigger evaluation**: Tool: `Bash (uv run python)`. Steps: Create TriggerCondition(condition_type="price_threshold", params={"token_pair":"ETH/USDC","threshold":3000,"direction":"above"}). Execute with price 3100. Expected: True. Evidence: `.sisyphus/evidence/task-13-price-trigger.txt`

## Final Verification Wave
F1. **Plan Compliance Audit** (`oracle`): Verify all 14 tasks addressed, guardrails enforced, deliverables match plan. Check: calldata allowlist active, Qdrant wrapped in to_thread, system prompt as constant, subgraph schema abstraction present, conditional routing defined, single invocation lock, no web3.py in agents.
F2. **Code Quality Review** (`unspecified-high`): ruff clean, mypy clean, no bare `except:`, all type hints present, `from __future__ import annotations` in all new files, LangGraph nodes have single responsibility, system prompt not inline.
F3. **Integration Smoke Test** (`unspecified-high`): Run full test suite: `uv run -m pytest tests/ --ignore=tests/soak --ignore=tests/integration/test_chaos.py -v`. Verify no regressions in Phase 1 (50) + Phase 2 (47) = 97 tests. Run Phase 3 tests: `uv run -m pytest tests/unit/test_agent_*.py tests/unit/test_calldata_safety.py tests/unit/test_graph_nodes.py tests/integration/test_agent_*.py -v`. Verify ~33 new tests pass.
F4. **LLM Validation Re-verification** (`oracle`): Re-run T0 POC to confirm endpoint is still healthy. Check that no real API calls leak into tests.
F5. **Scope Fidelity Check** (`deep`): Verify no web3.py imports in agent code. Verify dispatch is the ONLY path to ExecutionQueue. Verify no generic GraphQL strings accepted. Verify system prompt is imported from module, not hardcoded in graph.py.

## Commit Strategy
Single commit after all waves + final verification pass:
```
Phase 3: LangGraph multi-agent cognition layer

Cognitive layer bridging market analysis to MegaETH execution:
- app/agents/state.py: LangGraph AgentState + AgentContext TypedDicts
- app/agents/system_prompt.py: Agent task, tool usage, safety, output spec
- app/agents/tools/subgraph.py: Async gql client with fixed queries,
  schema abstraction (Uniswap V3 dev / Envio HyperIndex production)
- app/agents/tools/local_state.py: Portfolio & balance query tools
- app/agents/tools/memory.py: Cognitive memory search via Qdrant
  (sync calls wrapped in asyncio.to_thread)
- app/agents/tools/dispatch.py: ExecutionPayload enqueue with
  function-selector allowlist calldata safety layer
- app/agents/graph.py: Two-agent StateGraph (Market Intelligence →
  Portfolio Router), conditional routing, error recovery
- app/agents/runner.py: Background AgentRunner polling loop with
  idle backoff, single-invocation lock, graceful shutdown
- app/api/dependencies.py: Compiled graph singleton replaces stub
- app/api/routes/agent.py: POST /agent/invoke debug endpoint
- app/api/main.py: AgentRunner as 7th supervisor task
- app/agents/dispatcher.py: price_threshold + time_bound evaluators
- Config: OpenCode Go LLM settings, subgraph type, polling params
- Metrics: 6 new Prometheus counters for agent observability
- Tests: ~33 tests (unit: tools/safety/state/nodes;
  integration: graph/runner/API)
```

## Success Criteria
- [ ] LLM validation POC confirms `with_structured_output()` works with `deepseek-v4-flash`
- [ ] Graph compiles and invokes without crash (even with mock LLM)
- [ ] Calldata safety layer blocks known dangerous selectors (transferFrom, safeTransferFrom)
- [ ] Qdrant search uses `asyncio.to_thread()` (no event loop blocking)
- [ ] Conditional routing: opportunity → portfolio → dispatch; no opportunity → END
- [ ] Background polling loop runs at configured interval with idle backoff
- [ ] Supervisor starts all 7 tasks without conflict
- [ ] `POST /agent/invoke` returns agent state for debugging
- [ ] `price_threshold` and `time_bound` TriggerConditions evaluated correctly
- [ ] ruff clean, mypy clean
- [ ] ~33 new tests pass; no regression in 97 existing tests
- [ ] No web3.py imports in agent code
- [ ] No generic GraphQL query strings accepted
- [ ] System prompt is a module-level constant, not inline in graph.py
