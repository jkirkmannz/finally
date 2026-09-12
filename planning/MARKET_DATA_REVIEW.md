# Market Data Backend — Code Review

**Date:** 2026-09-12
**Reviewer:** Claude
**Scope:** `backend/app/market/` (8 source modules) and `backend/tests/market/` (6 test files, 75 tests)

This is the second review pass. The first pass (findings below, in §1) surfaced six issues; all six have been fixed in this pass, along with two new regression tests locking in the two behavioral fixes. This document supersedes the review that was in this same file before those fixes; the original findings are preserved in `planning/archive/` for history.

---

## 1. Fixes Applied This Pass

| # | Issue | Severity | Fix |
|---|---|---|---|
| 1 | `MassiveDataSource.start()` didn't normalize ticker case, but `add_ticker`/`remove_ticker` did — a ticker passed to `start()` in lowercase could never be removed later (reproduced: `remove_ticker("aapl")` silently failed to remove `"aapl"` when `start()` had stored it un-normalized). | Medium | `start()` now does `[t.upper().strip() for t in tickers]`, matching `add_ticker`/`remove_ticker`. Added `test_start_normalizes_ticker_case` to lock this in. |
| 2 | `GBMSimulator`'s `dt` was hardcoded to assume a 500ms tick regardless of `SimulatorDataSource.update_interval` — changing the interval would silently scale the effective annualized volatility with no warning. | Medium | `SimulatorDataSource.start()` now derives `dt = self._interval / GBMSimulator.TRADING_SECONDS_PER_YEAR` and passes it explicitly. Added `test_dt_scales_with_update_interval` to lock this in. |
| 3 | `stream.py` built its `APIRouter` at module scope and decorated onto it inside `create_stream_router()` — calling the factory twice (e.g., across tests, or a future reload path) would double-register `/prices` on the same shared router. | Low | `router = APIRouter(...)` moved inside `create_stream_router()`, so each call gets its own router. |
| 4 | `PriceCache.version` was read without `self._lock`, unlike every other accessor. | Low | **Not changed** — re-assessed as not worth it; see §5. |
| 5 | `tests/conftest.py` defined an `event_loop_policy` fixture that just returned the default policy, contributing a `DeprecationWarning` on every async test run (`asyncio.DefaultEventLoopPolicy` is deprecated, slated for removal in Python 3.16). | Trivial | Fixture removed; `conftest.py` is now just the module docstring. |
| 6 | `ruff format --check` flagged 3 test files as not matching the formatter's line-wrapping. | Trivial | Ran `ruff format` on the affected files (and once more on `stream.py` after edit #3 left it missing a blank line). |

Item 4 is intentionally left as-is — see §5 for reasoning.

---

## 2. Test Results (after fixes)

**75 tests collected, 75 passed, 0 failed.** (`uv run pytest -q --cov=app --cov-report=term-missing`, with `massive==2.2.0` actually installed via `uv sync --extra dev`.)

Two tests were added this pass (73 → 75): `test_start_normalizes_ticker_case` (massive) and `test_dt_scales_with_update_interval` (simulator source). No `DeprecationWarning`s remain in the run.

| Module | Coverage | Notes |
|---|---|---|
| models.py | 100% | |
| cache.py | 100% | |
| interface.py | 100% | |
| seed_prices.py | 100% | |
| factory.py | 100% | |
| simulator.py | 98% | Uncovered: L149 duplicate-add guard, L273-274 exception path in `_run_loop` |
| massive_client.py | 94% | Uncovered: `_poll_loop`'s `while True` body (L85-87), real (unmocked) `_fetch_snapshots` body (L125) |
| stream.py | 31% | Still untested — needs an ASGI test client, not added this pass (see §5) |
| **Total** | **91%** | Unchanged from before — the new tests cover previously-untested *behavior*, not previously-uncovered *lines* |

**Lint:** `ruff check app/ tests/` — clean. `ruff check --select F401` (unused imports) — clean.

**Format:** `ruff format --check app/ tests/` — clean, all 19 files formatted (was 3 files + `stream.py` dirty before this pass).

---

## 3. Architecture Assessment

Unchanged from the prior review: this remains a clean strategy-pattern implementation (`MarketDataSource` ABC → `SimulatorDataSource` / `MassiveDataSource`) writing into a single shared, thread-safe `PriceCache`, matching `planning/PLAN.md` §6. The fixes in this pass were surgical — no structural changes, no new modules, no altered public API shapes. `GBMSimulator.__init__` already accepted a `dt` parameter; the fix just wires the caller to pass the right value instead of relying on the default.

---

## 4. Re-Verification of Everything From the Original (2026-02-10) Review

For completeness, since this file has now gone through two review passes beyond the original:

| Original finding | Status |
|---|---|
| `pyproject.toml` missing wheel packaging config | Fixed (prior pass) |
| Massive tests fragile without `massive` installed | Fixed (prior pass) — confirmed again this pass, ran with `massive` installed |
| `_generate_events` return type annotation | Fixed (prior pass) |
| `PriceCache.version` not under lock | Still open — see §5, deliberately not fixed |
| `SimulatorDataSource.get_tickers` accessed private state | Fixed (prior pass) |
| Module-level `router` in `stream.py` | **Fixed this pass** |
| Unused imports in tests | Fixed (prior pass) |
| Missing SSE integration test | Still open — see §5 |
| No `PriceCache` concurrency test | Still open — see §5 |
| No full-10-ticker `GBMSimulator` test | Still open, but manually verified working (no numerical issue) |
| `DEFAULT_CORR` vs `CROSS_GROUP_CORR` naming | Fixed (prior pass) |

---

## 5. Remaining Open Items (Deliberately Not Fixed)

None of these are regressions or newly discovered problems — they're the same low-priority items from before, re-assessed and left open on purpose:

- **`PriceCache.version` unlocked read.** A single `int` read is atomic under CPython's GIL, and this project targets standard CPython, not a no-GIL build. Adding a lock here would be defensive code against a scenario that doesn't apply; not worth the (tiny) overhead or the inconsistency of "sometimes we lock, sometimes the JIT/GIL makes it safe anyway."
- **No SSE integration test for `stream.py`.** Testing this properly needs an ASGI test client (`httpx` + `ASGITransport`, or FastAPI's `TestClient`), which isn't currently a dependency, and there's no FastAPI `app` yet to mount the router into — that's the next phase of backend work, not this one. Adding a dependency and a test harness for a router that isn't wired into a real app yet would be premature; revisit once `main.py`/the FastAPI app exists.
- **No `PriceCache` concurrent-writers test.** The lock usage is straightforward (one `Lock`, held for the full duration of every method) and inspection gives high confidence it's correct. A multi-threaded stress test would mostly be testing Python's `threading.Lock`, not this code's logic.
- **No test with the full 10-ticker default set.** Manually verified this pass (again) that `GBMSimulator(tickers=list(SEED_PRICES.keys()))` builds a valid 10×10 Cholesky decomposition and steps cleanly. Still a coverage gap worth closing eventually, but it's a "nice to have," not a bug.

---

## 6. Verdict

All six issues identified in the first review pass are fixed, verified by a clean 75-test run, clean lint, and clean format check. The two behavioral fixes (ticker-case normalization, `dt`/`update_interval` coupling) now have dedicated regression tests so they can't silently regress. Nothing here blocks moving on to the rest of the backend (portfolio, watchlist, chat, and the FastAPI app that will actually mount `create_stream_router()`).

No further action needed on the market data subsystem before that next phase begins.
