# Market Data Backend — Code Review

**Date:** 2026-09-12
**Reviewer:** Claude
**Scope:** `backend/app/market/` (8 source modules) and `backend/tests/market/` (7 test files, 84 tests)

This is the third pass on this document. The first pass found six issues (§1 below); the second pass fixed all six and added regression tests for the two behavioral bugs. This third pass closes out the remaining items that were previously left open on purpose (§2): an SSE integration test, a `PriceCache` concurrency test, and a full-10-ticker `GBMSimulator` test. The market data subsystem is now considered complete and ready for the rest of the backend to build on.

---

## 1. Issues Fixed (Second Pass)

| # | Issue | Severity | Fix |
|---|---|---|---|
| 1 | `MassiveDataSource.start()` didn't normalize ticker case, but `add_ticker`/`remove_ticker` did — a ticker passed to `start()` in lowercase could never be removed later. | Medium | `start()` now does `[t.upper().strip() for t in tickers]`. Locked in by `test_start_normalizes_ticker_case`. |
| 2 | `GBMSimulator`'s `dt` was hardcoded to assume a 500ms tick regardless of `SimulatorDataSource.update_interval`. | Medium | `SimulatorDataSource.start()` derives `dt = self._interval / GBMSimulator.TRADING_SECONDS_PER_YEAR`. Locked in by `test_dt_scales_with_update_interval`. |
| 3 | `stream.py` built its `APIRouter` at module scope; calling `create_stream_router()` twice would double-register `/prices`. | Low | `router = APIRouter(...)` now built inside `create_stream_router()`. Locked in by `test_builds_independent_routers`. |
| 4 | `PriceCache.version` read without `self._lock`. | Low | Left as-is — see §4. |
| 5 | `tests/conftest.py`'s `event_loop_policy` fixture was a deprecated no-op, producing a `DeprecationWarning` on every async test. | Trivial | Fixture removed. |
| 6 | Formatting drift flagged by `ruff format --check` in several test files. | Trivial | `ruff format` applied. |

---

## 2. Improvements Added (This Pass)

These were the items previously listed as "worth doing, not urgent" / "deliberately not fixed" because they needed infrastructure the subsystem didn't have yet at the time (an ASGI test harness) or were judged lower value. All three are now done:

### 2.1 SSE integration test for `stream.py` (was 31% coverage, no tests)

Getting this right took a real detour worth recording: the natural first attempt — `httpx.ASGITransport` and, separately, FastAPI's `TestClient` — both **deadlock** against this endpoint. `_generate_events` is an unbounded `while True` loop that only exits when it observes `request.is_disconnected()`. Both of those test clients fully run the ASGI call to completion (buffering the entire response) *before* handing anything back to the caller to consume — there is no mechanism for the client to signal a disconnect mid-stream, so the server-side generator never sees one and the client never gets anything back. Confirmed this empirically with `faulthandler`-dumped stack traces showing both hung inside the initial `send()`/`handle_request()` call, before the streaming body was ever reached.

The fix was to test `_generate_events` directly: a minimal fake `Request` (just `.client.host` and a controllable `is_disconnected()`) drives the async generator with `__anext__()`, so the test controls disconnection deterministically instead of depending on transport-level streaming semantics that don't exist in either test client. `create_stream_router()`'s route-building and the endpoint's `StreamingResponse`/headers are tested separately by calling the routed endpoint function directly (via `router.routes[0].endpoint`) without consuming its body.

New file: `tests/market/test_stream.py`, 7 tests:
- Router factory builds independent routers per call (locks in fix #3 above)
- Endpoint returns a `StreamingResponse` with the right media type and headers
- First event is the `retry: 1000` directive
- Initial data event reflects whatever's already in the cache
- A cache update after the connection opens streams through as a new event
- No data event is ever produced while the cache stays empty across several ticks
- The generator stops (raises `StopAsyncIteration`) on disconnect

`stream.py` coverage: **31% → 94%** (only the `asyncio.CancelledError` logging branch remains uncovered — that requires actually cancelling the task rather than a clean disconnect, which is a real server-shutdown path, not something worth engineering a test around).

Added `httpx>=0.27.0` to `[project.optional-dependencies].dev` — it's what `fastapi.testclient.TestClient` needs even though it isn't used directly in the final tests; harmless to keep since it's dev-only and a natural fit for future API testing.

### 2.2 `PriceCache` concurrent-writers test

`tests/market/test_cache.py::test_concurrent_updates_are_not_lost` spins up 8 real OS threads (matching how the cache is actually used — `MassiveDataSource` calls into it via `asyncio.to_thread`), each performing 200 `update()` calls across 10 shared tickers, then asserts `cache.version` exactly equals `8 * 200 = 1600`. This is a meaningful assertion, not a smoke test: a broken lock (or a `+=` race) would show up here as a version count *less than* 1600 — a lost update — with high probability under real thread interleaving. Ran the full suite three times back-to-back to confirm no flakiness.

### 2.3 Full 10-ticker `GBMSimulator` test

`tests/market/test_simulator.py::test_full_default_watchlist_builds_valid_cholesky` builds a `GBMSimulator` with all 10 tickers from `SEED_PRICES` (mixing the tech group, the finance group, and TSLA's special-cased correlation all at once — the case none of the existing 1-2 ticker tests exercised), asserts the Cholesky decomposition is a proper 10×10 matrix, and runs 50 steps confirming all tickers stay present and positive. This had already been manually verified working in the first review pass; it's now a permanent regression test.

---

## 3. Test Results (Final)

**84 tests collected, 84 passed, 0 failed.** (`uv run pytest -q --cov=app --cov-report=term-missing`, `massive` installed via `uv sync --extra dev`.) Verified stable across 3 consecutive runs.

| Module | Coverage | Notes |
|---|---|---|
| models.py | 100% | |
| cache.py | 100% | |
| interface.py | 100% | |
| seed_prices.py | 100% | |
| factory.py | 100% | |
| simulator.py | 98% | Uncovered: L149 duplicate-add guard, L273-274 exception path in `_run_loop` |
| massive_client.py | 94% | Uncovered: `_poll_loop`'s `while True` body, real (unmocked) `_fetch_snapshots` body |
| stream.py | 94% | Uncovered: `asyncio.CancelledError` logging branch (server-shutdown path) |
| **Total** | **97%** | Up from 91% at the start of this pass |

**Lint:** `ruff check app/ tests/` — clean.
**Format:** `ruff format --check app/ tests/` — clean, all 20 files formatted.
**Dependency sanity:** `uv sync` (prod-only) and `uv sync --extra dev` both verified to install cleanly from a fresh lockfile resolution.

---

## 4. Remaining Open Item

- **`PriceCache.version` unlocked read.** Still deliberately left as-is. A single `int` read is atomic under CPython's GIL, this project targets standard CPython, and adding a lock here would be defensive code against a scenario (a no-GIL Python build) this project doesn't target. Re-affirmed in this pass; no plan to change unless the project's Python target changes.

---

## 5. Verdict

The market data backend is complete, tested, and ready. All issues from both prior review passes are resolved except the one item in §4, which is a deliberate judgment call rather than an oversight. Coverage is 97% overall, with every module except the two background polling loops (whose bodies are `await asyncio.sleep()` + a call already tested directly) above 90%. Nothing here should block building the rest of the backend — portfolio, watchlist, chat, and the FastAPI `app` that will mount `create_stream_router()` for real.
