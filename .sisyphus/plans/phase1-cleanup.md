# Phase 1 Cleanup

## TL;DR
> **Quick Summary**: Address 6 Oracle-identified gaps — summarizer unit tests, cohere-degraded chaos test, dependencies module, pyright config, cohere port alignment, composite PK documentation.
> **Deliverables**: 3 new files (summarizer tests, dependencies module, cohere chaos test), 2 config fixes, 1 documentation update
> **Estimated Effort**: Quick
> **Parallel Execution**: YES — all tasks independent
> **Critical Path**: None

## Context
### Original Request
Oracle post-implementation review found 5 minor gaps in Phase 1. Fix them in a cleanup sprint before Phase 2 begins.

### Decisions Made
- Use **Pyright** (not mypy) for type checking config
- Keep cohere-degraded chaos test minimal (same pattern as existing chaos tests)
- Composite PK already documented in AGENTS.md — just verify

### Metis Review
- C2: Use counter-based assertion (`degraded_events_total`), not log capture
- C4: Keep mypy only (already passing), remove pyright from scope
- C6: Document in `app/schemas/state.py` docstring (already 80% documented)
- C4: Run `ruff check --fix` immediately after adding config to fix any pre-existing violations
- C5: `.env` port change is local-dev only (docker-compose.yml already overrides for containers)

## TODOs

- [ ] C1. Add summarizer unit tests
  **What to do**: Create `tests/unit/test_summarizer.py` with 4-5 tests for `build_summary()` edge cases.
  - `test_empty_block` — zero transactions → `"Block #{n}: empty"`
  - `test_normal_block` — N transactions → `"Block #{n}: N transactions, X gas used, timestamp T"`
  - `test_500_char_boundary` — very long output gets truncated at 500 chars
  - `test_large_tx_count` — blocks with many transactions produce valid summaries
  **Must NOT do**: Import test infrastructure from Phase 1 integration tests. Keep unit tests pure (no Docker, no DB, no Redis).
  **Parallelization**: Independent, blocks nothing, blocked by nothing
  **Acceptance**: `uv run -m pytest tests/unit/test_summarizer.py -v` passes

- [ ] C2. Add cohere-degraded chaos test
  **What to do**: Add `test_cohere_degraded_mode` to `tests/integration/test_chaos.py` following the existing pattern. Reuse `_docker_stop` / `_docker_start` helpers.
  - Stop cohere-embed container
  - Inject blocks into Redis Stream
  - Run vector worker briefly → assert `degraded_events_total` counter increments
  - Verify worker does NOT crash (counter assertion, not log parsing)
  - Restart cohere-embed → assert `embeddings_computed` counter increments → verify normal operation resumes
  **Must NOT do**: Duplicate Docker control logic. Use counter-based assertions only (no log capture/grep).
  **Parallelization**: Independent, blocks nothing, blocked by nothing
  **Acceptance**: `SKIP_CHAOS_TESTS=0 uv run -m pytest tests/integration/test_chaos.py::TestChaosRecovery::test_cohere_degraded_mode -v -s` passes

- [ ] C3. Create `app/api/dependencies.py`
  **What to do**: Create an empty module `app/api/dependencies.py` with a docstring, ready for shared FastAPI dependencies when Phase 2 adds new routes.
  **Must NOT do**: Add any actual dependency functions (Phase 2's job).
  **Parallelization**: Independent, blocks nothing, blocked by nothing

- [ ] C4. Add `[tool.ruff]` to pyproject.toml
  **What to do**: Add ruff config section:
  ```toml
  [tool.ruff]
  target-version = "py311"
  line-length = 120
  lint.select = ["E", "F", "I"]

  [tool.ruff.lint.isort]
  known-first-party = ["app"]
  ```
  After adding config, run `uv run ruff check . --fix` to fix any pre-existing violations.
  **Must NOT do**: Add `[tool.pyright]` or `[tool.mypy]` — mypy is already passing and configured elsewhere.
  **Parallelization**: Independent, blocks nothing, blocked by nothing
  **Acceptance**: `uv run ruff check .` reports 0 errors; all tests still pass

- [ ] C5. Align cohere-embed port in `.env`
  **What to do**: The `docker-compose.override.yml` maps cohere-embed to host port `8001` (not `8000`). Update `.env` to use `http://127.0.0.1:8001` so host-based integration tests can reach the embedder.
  **Must NOT do**: Change the override port — only align `.env` to match it.
  **Parallelization**: Independent, blocks nothing, blocked by nothing

- [ ] C6. Clarify composite PK documentation in `app/schemas/state.py`
  **What to do**: The docstring already documents the composite PK. Add one clarifying sentence: "The `(block_number, index)` composite primary key supersedes the `payload_id` field from the original Phase 1 plan — `mini_block_number` serves as the unique deduplication key instead."
  **Parallelization**: Independent, blocks nothing, blocked by nothing
  **Acceptance**: `uv run -m pytest tests/unit/test_schemas.py::TestMiniBlockRecord -v` — all tests pass; docstring contains the note

## Success Criteria
1. All 6 cleanup items complete
2. Test count increases by 4-5 unit + 1 chaos test
3. `uv run -m pytest tests/ -v` — all tests pass
4. `uv run ruff check app/` — zero errors
5. Pyright config present in pyproject.toml
