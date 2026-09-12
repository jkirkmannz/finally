# Market Data Backend — Design Document

Implementation-ready design for the FinAlly market data subsystem: a unified
interface with two interchangeable implementations (a GBM simulator and a
Massive/Polygon.io REST poller), a thread-safe price cache, and an SSE
streaming endpoint. This document reflects the **as-built** code in
`backend/app/market/` (8 modules) — it is the authoritative reference for
any agent wiring the rest of the backend (portfolio, watchlist, chat) on top
of live prices.

Status: this subsystem is complete and tested (see
`planning/MARKET_DATA_SUMMARY.md`). `backend/app/main.py` — the FastAPI app
that wires this subsystem into a running server — does not exist yet; §10
below is the design for building it.

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model)
4. [Price Cache — `cache.py`](#4-price-cache)
5. [Abstract Interface — `interface.py`](#5-abstract-interface)
6. [Seed Prices & Ticker Parameters — `seed_prices.py`](#6-seed-prices--ticker-parameters)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client)
9. [Factory — `factory.py`](#9-factory)
10. [FastAPI Integration — `stream.py` and `main.py`](#10-fastapi-integration)
11. [Watchlist Coordination](#11-watchlist-coordination)
12. [Testing Strategy](#12-testing-strategy)
13. [Error Handling & Edge Cases](#13-error-handling--edge-cases)
14. [Configuration Summary](#14-configuration-summary)

---

## 1. Architecture

```
                     MarketDataSource (ABC)
                    /                      \
        SimulatorDataSource          MassiveDataSource
        (GBM, in-process,             (Polygon.io REST
         no external deps)             poller, needs API key)
                    \                      /
                     v                    v
                       PriceCache
                (thread-safe, in-memory,
                 versioned for change detection)
                    /          |          \
                   v           v           v
          SSE /api/stream   Portfolio    Trade
             /prices        valuation   execution
```

**Strategy pattern.** Both data sources implement `MarketDataSource`.
Downstream code (SSE streaming, portfolio valuation, trade execution) never
knows or cares which one is active — it only ever talks to the shared
`PriceCache`.

**Push model, not pull.** A data source is not asked for a price; it writes
prices into the cache on its own schedule (simulator: every 500ms, Massive:
every 15s by default). Readers poll the cache at whatever cadence they need,
decoupled from the producer's timing.

**Selection is environment-driven.** `create_market_data_source()` picks the
implementation based on whether `MASSIVE_API_KEY` is set — no code change
needed to switch between simulated and real data (see [PLAN.md §6](PLAN.md)).

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py             # Re-exports: PriceUpdate, PriceCache, MarketDataSource,
                               #   create_market_data_source, create_stream_router
      models.py                # PriceUpdate dataclass
      cache.py                 # PriceCache (thread-safe in-memory store)
      interface.py              # MarketDataSource ABC
      seed_prices.py            # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, CORRELATION_GROUPS
      simulator.py               # GBMSimulator + SimulatorDataSource
      massive_client.py          # MassiveDataSource
      factory.py                 # create_market_data_source()
      stream.py                  # SSE endpoint (FastAPI router factory)
  market_data_demo.py           # Rich terminal demo (uv run market_data_demo.py)
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
```

Each module has a single responsibility. `app/market/__init__.py` re-exports
the public API so the rest of the backend imports from `app.market` without
reaching into submodules:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source
```

---

## 3. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only data structure that leaves the market data layer.
Every downstream consumer — SSE streaming, portfolio valuation, trade
execution — works exclusively with this type.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True`** — price updates are immutable value objects; once
  created they never change, so they're safe to share across async tasks
  without copying.
- **`slots=True`** — memory optimization; many of these are created per
  second.
- **Computed properties** (`change`, `change_percent`, `direction`) are
  derived from `price`/`previous_price` so they can never drift out of sync
  with each other — there is no stale `direction` field to forget to update.
- **`to_dict()`** is the single serialization point used by both the SSE
  endpoint and any future REST API response.

---

## 4. Price Cache

**File: `backend/app/market/cache.py`**

The central data hub. Data sources write to it; SSE streaming and (later)
portfolio valuation / trade execution read from it. It must be thread-safe
because the Massive client's synchronous calls run in a thread-pool
executor via `asyncio.to_thread`, while SSE reads happen on the event loop.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price
        (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why a version counter

The SSE loop polls the cache every ~500ms. Without a version counter it
would serialize and send all prices on every tick even when nothing
changed (e.g. Massive only updates every 15s). The counter lets the SSE
loop skip sends when there's nothing new:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

### Thread safety rationale

`threading.Lock` is used instead of `asyncio.Lock` because:

- The Massive client's synchronous `get_snapshot_all()` runs via
  `asyncio.to_thread()`, which executes in a real OS thread —
  `asyncio.Lock` would not protect against that.
- `threading.Lock` works correctly from both sync threads and the async
  event loop, so one cache implementation serves both data sources.

---

## 5. Abstract Interface

**File: `backend/app/market/interface.py`**

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Why the source writes to the cache instead of returning prices

This push model decouples timing. The simulator ticks at 500ms, Massive
polls at 15s, but SSE always reads from the cache at its own 500ms cadence.
The SSE layer never needs to know which data source is active or what its
update interval is.

---

## 6. Seed Prices & Ticker Parameters

**File: `backend/app/market/seed_prices.py`**

Constants only — no logic, no imports beyond stdlib. Shared by the
simulator (initial prices and GBM parameters) and available as a fallback
reference for any code that wants a sane starting price for an unknown
ticker.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

Tickers added dynamically that aren't in `SEED_PRICES`/`TICKER_PARAMS` fall
back to a random seed price between $50–$300 and `DEFAULT_PARAMS`.

---

## 7. GBM Simulator

**File: `backend/app/market/simulator.py`**

Two classes live here: `GBMSimulator` (pure math engine, stateful) and
`SimulatorDataSource` (the `MarketDataSource` implementation that wraps it
in an async loop and writes to the `PriceCache`).

### 7.1 The math

Geometric Brownian Motion is the standard model underlying Black-Scholes:
prices evolve continuously with random noise, can never go negative, and
follow the lognormal distribution seen in real markets.

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

- `S(t)` — current price
- `mu` — annualized drift (expected return), e.g. `0.05`
- `sigma` — annualized volatility, e.g. `0.20`
- `dt` — time step as a fraction of a trading year
- `Z` — a (correlated) standard normal random variable

For 500ms ticks over a 252-day, 6.5-hour trading year:

```
dt = 0.5 / (252 * 6.5 * 3600) ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick that accumulate naturally
into realistic intraday ranges over time. Prices can never go negative
because the update is multiplicative through `exp()`.

### 7.2 Correlated moves via Cholesky decomposition

Real stocks don't move independently — tech stocks tend to move together.
Given a correlation matrix `C`, compute `L = cholesky(C)`; for independent
standard normals `Z_independent`, `Z_correlated = L @ Z_independent` gives
draws with the desired correlation structure. Cholesky decomposition
requires the matrix be positive semi-definite, which holds for any valid
correlation matrix (all diagonal 1s, off-diagonal in `[-1, 1]`, symmetric).

Correlation structure used here:

| Pair | Correlation |
|---|---|
| Tech ↔ tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Finance ↔ finance (JPM, V) | 0.5 |
| TSLA ↔ anything | 0.3 (it does its own thing) |
| Cross-sector / unknown tickers | 0.3 |

### 7.3 Random shock events

Each tick, each ticker has a small probability (default `0.001`) of a
sudden 2–5% move — visual drama for the demo. With 10 tickers at 2
ticks/sec, expect an event roughly every 50 seconds.

### 7.4 Full implementation

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
    """

    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        Hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker, shock_magnitude * 100, "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in the tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### Key behaviors

- **Immediate seeding** — `start()` populates the cache with seed prices
  *before* the loop begins, so the SSE endpoint has data to send on its
  very first tick (no blank-screen delay).
- **Graceful cancellation** — `stop()` cancels the task and awaits it,
  swallowing `CancelledError`, for clean shutdown during FastAPI lifespan
  teardown.
- **Exception resilience** — the loop catches exceptions per-step so one
  bad tick doesn't kill the entire feed.
- **`GBMSimulator.get_tickers()`** is a public method — `SimulatorDataSource`
  never reaches into a private attribute to expose the ticker list.

---

## 8. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls the Massive (formerly Polygon.io) REST API snapshot endpoint on a
configurable interval. The client is synchronous, so it runs inside
`asyncio.to_thread()` to avoid blocking the event loop.

### 8.1 Massive API primer

- **Package**: `massive` (declared as a core dependency in
  `backend/pyproject.toml`; `uv add massive`)
- **Auth**: `RESTClient(api_key=...)` — reads `MASSIVE_API_KEY` automatically
  if omitted
- **Rate limits**: free tier 5 req/min → poll every 15s; paid tiers support
  polling every 2–5s
- **Primary endpoint**: `GET /v2/snapshot/locale/us/markets/stocks/tickers`
  — returns current data for *all requested tickers in one call*, which is
  what keeps us within the free-tier rate limit regardless of watchlist size

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="...")
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)
for snap in snapshots:
    print(snap.ticker, snap.last_trade.price, snap.last_trade.timestamp)
```

Relevant response fields per ticker: `last_trade.price` (current price used
for trading/display), `last_trade.timestamp` (Unix **milliseconds**),
`day.previous_close` / `day.change_percent` (available if a day-change UI
element is added later).

### 8.2 Implementation

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers), self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds -> seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop retries on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

Note: imports are at module level (not lazy) because `massive` is a core
dependency of `backend/pyproject.toml` — the simulator path still has zero
*runtime* dependency on it being *configured* (no API key → this module is
simply never instantiated), but the package itself is always installed.

### 8.3 Error handling philosophy

The poller is intentionally resilient — a bad response never takes down the
price feed:

| Error | Behavior |
|---|---|
| **401 Unauthorized** | Logged as error; poller keeps running (user might fix `.env` and restart). |
| **429 Rate Limited** | Logged as error; next poll retries after `poll_interval` seconds. |
| **Network timeout** | Logged as error; retries automatically on next cycle. |
| **Malformed snapshot** | That ticker is skipped with a warning; other tickers in the same response are still processed. |
| **All tickers fail** | Cache retains last-known prices; SSE keeps streaming stale data (better than no data). |

---

## 9. Factory

**File: `backend/app/market/factory.py`**

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty -> MassiveDataSource (real market data)
    - Otherwise -> SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

Usage at app startup:

```python
price_cache = PriceCache()
source = create_market_data_source(price_cache)
await source.start(initial_tickers)  # e.g., ["AAPL", "GOOGL", ...]
```

---

## 10. FastAPI Integration

### 10.1 SSE Streaming Endpoint — `stream.py` (built)

**File: `backend/app/market/stream.py`**

A FastAPI route that holds open a long-lived HTTP connection and pushes
price updates to the client as `text/event-stream`.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    """

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    yield "retry: 1000\n\n"  # Reconnect after 1s on drop

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

**Wire format** — each event looks like:

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Client-side (`EventSource`, native browser API — no library needed):

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices is { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp }, ... }
};
```

**Why poll-and-push instead of event-driven?** The endpoint polls the cache
on a fixed interval rather than being notified by the data source
directly. This is simpler and produces evenly-spaced updates, which matters
because the frontend accumulates them into sparkline charts — regular
spacing keeps that visualization clean regardless of which backend data
source is active.

### 10.2 Lifecycle Integration — `main.py` (not yet built)

`backend/app/main.py` does not exist yet. This is the design for wiring
the market data subsystem into the FastAPI app via the `lifespan` context
manager, so whoever builds the rest of the backend (portfolio, watchlist,
chat routes) has a concrete pattern to follow.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router
from app.market.interface import MarketDataSource


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown of background services."""

    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # Load initial tickers from the database watchlist (lazily initializes
    # the DB and seeds default tickers on first run — see PLAN.md §7)
    initial_tickers = await load_watchlist_tickers()
    await source.start(initial_tickers)

    stream_router = create_stream_router(price_cache)
    app.include_router(stream_router)

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

Other routes access the price cache and data source via dependency
injection:

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(404, f"No price available for {trade.ticker}")
    # ... execute trade at current_price ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... insert into watchlist table ...
    await source.add_ticker(payload.ticker)
    # ...


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... delete from watchlist table ...
    await source.remove_ticker(ticker)
    # ...
```

---

## 11. Watchlist Coordination

When the watchlist changes (via REST API or LLM chat action), the market
data source must be told so it tracks the right set of tickers.

### Adding a ticker

```
User (or LLM) -> POST /api/watchlist {ticker: "PYPL"}
  -> Insert into watchlist table (SQLite)
  -> await source.add_ticker("PYPL")
       Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
       Massive:   appends to ticker list, appears on next poll (up to poll_interval delay)
  -> Return success (ticker + current price if available)
```

### Removing a ticker

```
User (or LLM) -> DELETE /api/watchlist/PYPL
  -> Delete from watchlist table (SQLite)
  -> await source.remove_ticker("PYPL")
       Simulator: removes from GBMSimulator, rebuilds Cholesky, removes from cache
       Massive:   removes from ticker list, removes from cache
  -> Return success
```

### Edge case: ticker still has an open position

If the user removes a ticker from the watchlist but still holds shares, the
data source must keep tracking it so portfolio valuation stays accurate.
The watchlist route is responsible for this check — the market data layer
itself has no concept of "positions":

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)

    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

---

## 12. Testing Strategy

**File location: `backend/tests/market/`** — 6 modules, 73 tests, 84%
overall coverage (see `planning/MARKET_DATA_SUMMARY.md` for the full
breakdown). Summary of what each module verifies:

| Module | Focus |
|---|---|
| `test_models.py` | `PriceUpdate` computed properties (`change`, `change_percent`, `direction`), `to_dict()` serialization, immutability |
| `test_cache.py` | update/get/get_all/remove, first-update-is-flat, direction on up/down, version increments on every write |
| `test_simulator.py` | `GBMSimulator.step()` always returns all tickers, prices stay positive over 10k steps, add/remove ticker rebuilds Cholesky, unknown ticker gets a random seed in range, empty ticker list is a no-op |
| `test_simulator_source.py` | `SimulatorDataSource.start()` seeds the cache before the first tick, prices change over time, `stop()` is idempotent, `add_ticker`/`remove_ticker` propagate to both the simulator and the cache |
| `test_factory.py` | `MASSIVE_API_KEY` set → `MassiveDataSource`; unset/empty → `SimulatorDataSource` |
| `test_massive.py` | `_poll_once` updates the cache from mocked snapshots, malformed snapshots are skipped without aborting the batch, API exceptions don't crash the poller |

Representative test (full suite is in the repo):

```python
# backend/tests/market/test_simulator.py
class TestGBMSimulator:
    def test_prices_are_positive(self):
        """GBM prices can never go negative (exp() is always positive)."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            prices = sim.step()
            assert prices["AAPL"] > 0

    def test_cholesky_rebuilds_on_add(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None  # Only 1 ticker, no correlation matrix
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None
```

```python
# backend/tests/market/test_massive.py
@pytest.mark.asyncio
class TestMassiveDataSource:
    async def test_malformed_snapshot_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "BAD"]

        good_snap = _make_snapshot("AAPL", 190.50, 1707580800000)
        bad_snap = MagicMock(ticker="BAD", last_trade=None)  # triggers AttributeError

        with patch.object(source, "_fetch_snapshots", return_value=[good_snap, bad_snap]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None
```

Run locally:

```bash
cd backend
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
```

### Gaps to be aware of

- `stream.py` has low direct coverage (31%) — exercising the SSE generator
  properly requires a running ASGI test client (e.g. `httpx.AsyncClient`
  against the FastAPI `app`), which isn't possible until `main.py` exists.
  Add an SSE integration test once the app is wired up in §10.2.
- No dedicated concurrent-writer stress test for `PriceCache` (lock
  correctness is verified by inspection, not empirically under contention).

---

## 13. Error Handling & Edge Cases

### 13.1 Startup with an empty watchlist

If the database has no watchlist entries, `start()` receives an empty
list. Both data sources handle this gracefully — the simulator produces no
prices, the Massive poller skips its API call entirely. The SSE endpoint
simply sends no events until a ticker is added, at which point tracking
starts immediately.

### 13.2 Price cache miss during a trade

If a user tries to trade a ticker with no cached price yet (just added,
Massive hasn't polled it):

```python
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(
        status_code=400,
        detail=f"Price not yet available for {ticker}. Please wait a moment and try again.",
    )
```

The simulator avoids this entirely by seeding the cache synchronously
inside `add_ticker()`. The Massive client may have a brief gap until its
next poll — the 400 with a clear message is the correct response there.

### 13.3 Massive API key invalid

If the key is set but wrong, the first poll fails with 401. The poller
logs the error and keeps retrying every `poll_interval`. SSE keeps
streaming (connected, just empty). The fix is correcting `.env` and
restarting the container.

### 13.4 Thread safety under load

`PriceCache` uses a `threading.Lock` (a mutex). Under expected load (10
tickers, 2 updates/sec, one SSE reader per browser tab) contention is
negligible — the critical section is a dict lookup plus assignment. If this
ever became a bottleneck (hundreds of tickers, many concurrent readers) a
`ReadWriteLock` would be the fix, but that's unnecessary for this project's
scale.

### 13.5 Simulator numerical precision

The tiny `dt` produces very small per-tick moves; this is not a precision
concern because prices are rounded to 2 decimals in `GBMSimulator.step()`,
the `exp(drift + diffusion)` formulation is numerically stable, and prices
are always positive by construction (exponential of a real number).

---

## 14. Configuration Summary

| Parameter | Location | Default | Description |
|---|---|---|---|
| `MASSIVE_API_KEY` | Environment variable | `""` (empty) | If set, use Massive API; otherwise use the simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Time between simulator ticks |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Time between Massive API polls (free tier: 5 req/min) |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of a random shock event per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.5e-8` | GBM time step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5` s | Time between SSE pushes to a connected client |
| SSE retry directive | `_generate_events()` | `1000` ms | Browser `EventSource` reconnection delay |

### Package `__init__.py`

**File: `backend/app/market/__init__.py`**

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```
