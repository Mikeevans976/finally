# Market Data Backend — Design

The authoritative design for the FinAlly market data subsystem (`backend/app/market/`) and for how the rest of the backend plugs into it. It has code you can implement from directly, plus worked examples.

**How this relates to the other docs**

- `planning/PLAN.md` §6 sets the requirements. §2 of this doc checks each one off.
- `planning/MARKET_DATA_SUMMARY.md` describes what is already built and tested (8 modules, 73 passing tests).
- `planning/archive/*` holds the original research (Massive API, simulator math, interface sketch, code review). This doc replaces `archive/MARKET_DATA_DESIGN.md`.

Most of the subsystem already exists. This doc keeps what works, specifies a small set of **revisions** (R1–R7) found while checking the code against PLAN.md and the installed `massive` SDK, and designs the **integration layer** that doesn't exist yet: the app lifespan, watchlist and position coordination, and the consumer APIs.

---

## Table of Contents

1. [Revisions at a Glance](#1-revisions-at-a-glance)
2. [Requirements Traceability](#2-requirements-traceability)
3. [Architecture](#3-architecture)
4. [File Layout](#4-file-layout)
5. [Data Model — `models.py`](#5-data-model--modelspy)
6. [Price Cache — `cache.py`](#6-price-cache--cachepy)
7. [Abstract Interface — `interface.py`](#7-abstract-interface--interfacepy)
8. [Seed Data — `seed_prices.py`](#8-seed-data--seed_pricespy)
9. [GBM Simulator — `simulator.py`](#9-gbm-simulator--simulatorpy)
10. [Massive Client — `massive_client.py`](#10-massive-client--massive_clientpy)
11. [Factory — `factory.py`](#11-factory--factorypy)
12. [SSE Streaming — `stream.py`](#12-sse-streaming--streampy)
13. [Ticker Tracking — `tracking.py`](#13-ticker-tracking--trackingpy)
14. [App Integration — `main.py`](#14-app-integration--mainpy)
15. [Consumers: Watchlist, Trades, Portfolio, Chat](#15-consumers)
16. [Frontend Contract](#16-frontend-contract)
17. [Error Handling & Edge Cases](#17-error-handling--edge-cases)
18. [Testing](#18-testing)
19. [Configuration Reference](#19-configuration-reference)
20. [Implementation Checklist](#20-implementation-checklist)

---

## 1. Revisions at a Glance

| # | Area | Problem in current code | Fix |
|---|------|-------------------------|-----|
| **R1** | `massive_client.py` | Reads `snap.last_trade.timestamp`, but the SDK's `LastTrade` has no such field. Its timestamp is `sip_timestamp`, in **nanoseconds**. With the real API, every snapshot raises `AttributeError` and is skipped, so Massive mode yields **no prices**. The tests pass only because they hand-build `MagicMock` snapshots. | Parse real `TickerSnapshot` fields with a fallback chain (§10.3). Test against `TickerSnapshot.from_dict(...)` built from real JSON (§18.4). |
| **R2** | `models.py` / `cache.py` | PLAN.md asks for a **daily change %** in the watchlist. `change_percent` is tick-over-tick (~0.01%), which is meaningless for that column. | Add `reference_price` (previous close for Massive, session-start price for the simulator) and the derived `day_change` / `day_change_percent`. |
| **R3** | `stream.py` | `router` is created at module level and `create_stream_router()` adds a route to it on every call, so a second call (common in tests) registers `/prices` twice. | Create the `APIRouter` inside the factory. |
| **R4** | `stream.py` | Nothing is sent while the cache is unchanged. In Massive mode that means 15 s of silence, and some proxies drop idle connections. | Send an SSE comment heartbeat (`: ping`) every 15 s of idle time. |
| **R5** | `cache.py` | `version` is read without the lock. `update()` uses `timestamp or time.time()`, which treats `0.0` as missing. | Read under the lock. Test `timestamp is None`. |
| **R6** | All sources | Ticker symbols aren't normalized in `SimulatorDataSource`, so `"aapl"` and `"AAPL"` become separate series. | `normalize_ticker()` in `interface.py`, used by every entry point. |
| **R7** | Integration | Removing a ticker from the watchlist stops its prices even if the user still **holds** it, which breaks portfolio valuation. | One function, `sync_tracked_tickers()`, keeps the tracked set equal to *watchlist ∪ open positions* (§13). |

Everything else (GBM math, Cholesky correlation, shock events, version-based SSE, the factory, and the lifecycle contract) stays as built.

---

## 2. Requirements Traceability

| PLAN.md requirement | Where it is met |
|---|---|
| Simulator and Massive behind one interface; downstream code doesn't care which | `MarketDataSource` ABC (§7). Consumers only read `PriceCache` (§6). |
| `MASSIVE_API_KEY` set → Massive, else simulator | `create_market_data_source()` (§11) |
| GBM with per-ticker drift and volatility, ~500 ms updates | `GBMSimulator` (§9.2) |
| Correlated moves (tech moves together) | Cholesky of a sector correlation matrix (§9.3) |
| Occasional 2–5% "events" | Shock step, p = 0.001 per ticker per tick (§9.4) |
| Realistic seed prices | `SEED_PRICES` (§8) |
| Massive REST polling, union of watched tickers, 15 s on the free tier | `MassiveDataSource` with one batched snapshot call per poll (§10) |
| Shared cache: latest price, previous price, timestamp | `PriceCache` / `PriceUpdate` (§5–6) |
| `GET /api/stream/prices`, ~500 ms cadence, each event has ticker, price, previous price, timestamp, direction | `create_stream_router()` (§12) |
| Client reconnects automatically | `retry: 1000` directive plus native `EventSource` retry (§12, §16) |
| Watchlist shows daily change % | `day_change_percent` (R2, §5) |
| Prices for held positions (portfolio value, P&L, heatmap) | Tracked set = watchlist ∪ positions (R7, §13) |
| Trades fill instantly at the current price | `PriceCache.get_price()` (§15.2) |

---

## 3. Architecture

```
                         ┌────────────────────────────────────┐
  MASSIVE_API_KEY? ───▶  │ create_market_data_source(cache)   │
                         └───────────────┬────────────────────┘
                                         │ returns one of
                 ┌───────────────────────┴──────────────────────┐
                 ▼                                              ▼
   SimulatorDataSource                               MassiveDataSource
   asyncio task, every 0.5 s:                        asyncio task, every 15 s:
     GBMSimulator.step()                               to_thread(get_snapshot_all)
                 │                                              │
                 └──────────────┐                ┌──────────────┘
                                ▼  cache.update  ▼
                        ┌───────────────────────────────┐
                        │ PriceCache (threading.Lock)   │
                        │  ticker → PriceUpdate         │
                        │  version: int (monotonic)     │
                        └───┬──────────┬──────────┬─────┘
                            │          │          │      reads only
              SSE /api/stream/prices   │   portfolio valuation, snapshots,
              (one generator per       │   LLM context
               client, polls version)  │
                                       trade execution (fill price)

  Watchlist API / LLM actions / trades ──▶ sync_tracked_tickers(db, source)
                                           desired = watchlist ∪ positions
                                           → source.add_ticker / remove_ticker
```

**Rules**

1. **There is exactly one writer.** The active data source is the only thing that calls `cache.update()`. Routes never write prices.
2. **Consumers never call the source for prices.** They read the cache. The source is only told *which* tickers to track.
3. **No I/O on the event loop.** The Massive SDK is synchronous, so it runs in `asyncio.to_thread`. That is why the cache uses a `threading.Lock` and not an `asyncio.Lock`.
4. **Values are immutable.** `PriceUpdate` is a frozen dataclass, so a snapshot from `get_all()` can be shared freely.

**Concurrency model.** Everything runs on uvicorn's single event loop except the Massive HTTP call, which runs in the default thread pool. Critical sections in the cache are a dict lookup plus an assignment, so contention doesn't matter at this scale (tens of tickers, a handful of SSE clients).

---

## 4. File Layout

```
backend/
  app/
    main.py                  # NEW: FastAPI app, lifespan, static files, router wiring
    deps.py                  # NEW: get_price_cache / get_market_source dependencies
    market/
      __init__.py            # public API re-exports (adds normalize_ticker, sync_tracked_tickers)
      models.py              # PriceUpdate                       (R2)
      cache.py               # PriceCache                        (R2, R5)
      interface.py           # MarketDataSource, normalize_ticker (R6)
      seed_prices.py         # seed prices, GBM params, correlations (unchanged)
      simulator.py           # GBMSimulator, SimulatorDataSource  (R6)
      massive_client.py      # MassiveDataSource                  (R1, R2, R6)
      factory.py             # create_market_data_source          (poll interval env var)
      stream.py              # create_stream_router               (R3, R4)
      tracking.py            # NEW: sync_tracked_tickers          (R7)
  tests/market/
    test_models.py  test_cache.py  test_simulator.py  test_simulator_source.py
    test_factory.py test_massive.py
    test_stream.py           # NEW
    test_tracking.py         # NEW
```

---

## 5. Data Model — `models.py`

`PriceUpdate` is the only type that leaves the market data layer.

```python
"""Data models for market data."""

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
    # Baseline for the daily change: previous close (Massive) or the first
    # price seen this session (simulator). None means "same as price".
    reference_price: float | None = None

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

    @property
    def day_change(self) -> float:
        """Absolute change versus the reference (daily) price."""
        ref = self.reference_price if self.reference_price is not None else self.price
        return round(self.price - ref, 4)

    @property
    def day_change_percent(self) -> float:
        """Percentage change versus the reference (daily) price."""
        ref = self.reference_price if self.reference_price is not None else self.price
        if ref == 0:
            return 0.0
        return round((self.price - ref) / ref * 100, 4)

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
            "reference_price": self.reference_price,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

**Example**

```python
>>> u = PriceUpdate("AAPL", price=192.10, previous_price=192.05,
...                 timestamp=1760000000.0, reference_price=190.00)
>>> u.direction, u.change, u.change_percent
('up', 0.05, 0.026)
>>> u.day_change, u.day_change_percent
(2.1, 1.1053)
```

**Decisions**

- Derived values are `@property`s, so `direction` can never disagree with the prices.
- `reference_price` is optional with a default, so every existing constructor call and test keeps working.
- The keys in `to_dict()` are the wire contract with the frontend (§16). Add keys freely; never rename one.

---

## 6. Price Cache — `cache.py`

```python
"""Thread-safe in-memory price cache."""

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

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        previous_price is the last cached price (or `price` on first sight).
        reference_price, if not given, carries over from the previous update,
        or is pinned to `price` on first sight (= session open).
        """
        with self._lock:
            ts = time.time() if timestamp is None else timestamp
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None:
                reference_price = prev.reference_price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
                reference_price=round(reference_price, 2),
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
        """Remove a ticker from the cache (e.g., when no longer tracked)."""
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1  # so SSE clients see the removal

    @property
    def version(self) -> int:
        """Current version counter. Used by SSE for change detection."""
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Example: how the reference price behaves**

```python
cache = PriceCache()
cache.update("AAPL", 190.00)        # first sight: prev=190.00, ref=190.00, flat
cache.update("AAPL", 190.40)        # prev=190.00, ref=190.00 (carried), up
u = cache.update("AAPL", 189.90)    # prev=190.40, ref=190.00, down
u.day_change_percent                # -0.0526

# Massive passes the previous close explicitly on every poll:
cache.update("MSFT", 421.30, timestamp=1760000000.0, reference_price=418.00)
```

**Why `remove()` bumps the version.** Without the bump, an SSE client keeps showing a removed ticker until some *other* price changes. The simulator always has other prices changing, but in Massive mode that can take 15 s. Each SSE payload is a full snapshot (§12), so the removal shows up as the key simply being absent.

---

## 7. Abstract Interface — `interface.py`

The contract doesn't change. R6 adds one shared helper.

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

# 1-5 letters, optionally a class suffix: BRK.B, BF-B. Covers US equities.
_TICKER_RE = re.compile(r"^[A-Z]{1,5}([.\-][A-Z]{1,2})?$")


def normalize_ticker(ticker: str) -> str:
    """Upper-case and strip a ticker symbol. Raises ValueError if malformed."""
    t = ticker.strip().upper()
    if not _TICKER_RE.match(t):
        raise ValueError(f"Invalid ticker symbol: {ticker!r}")
    return t


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices;
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers. Call once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task. Idempotent. No cache writes afterwards."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and evict it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the tickers currently being tracked."""
```

**Where validation happens.** API routes call `normalize_ticker()` and turn `ValueError` into HTTP 400 (§15.1). The sources also normalize defensively, but they never raise for valid-looking symbols: `add_ticker("aapl")` and `add_ticker("AAPL")` do the same thing.

---

## 8. Seed Data — `seed_prices.py`

Unchanged. For reference:

| Ticker | Seed $ | σ (annual vol) | μ (annual drift) | Group |
|--------|-------:|------:|------:|-------|
| AAPL | 190 | 0.22 | 0.05 | tech |
| GOOGL | 175 | 0.25 | 0.05 | tech |
| MSFT | 420 | 0.20 | 0.05 | tech |
| AMZN | 185 | 0.28 | 0.05 | tech |
| TSLA | 250 | 0.50 | 0.03 | independent (ρ=0.3 with everything) |
| NVDA | 800 | 0.40 | 0.08 | tech |
| META | 500 | 0.30 | 0.05 | tech |
| JPM | 195 | 0.18 | 0.04 | finance |
| V | 280 | 0.17 | 0.04 | finance |
| NFLX | 600 | 0.35 | 0.05 | tech |
| *other* | U(50, 300) | 0.25 | 0.05 | ρ=0.3 with everything |

Correlations: tech–tech 0.6, finance–finance 0.5, anything involving TSLA 0.3, everything else 0.3.

To add a sector (say energy), add a set to `CORRELATION_GROUPS`, a constant such as `INTRA_ENERGY_CORR = 0.5`, and one branch in `_pairwise_correlation`. The matrix stays positive-definite as long as each intra-group ρ ≥ the cross-group ρ and all ρ < 1.

---

## 9. GBM Simulator — `simulator.py`

### 9.1 The math, with numbers

Each tick, every price evolves by exact (log-space) GBM:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
dt      = 0.5 s / (252 days × 6.5 h × 3600 s) = 0.5 / 5,896,800 ≈ 8.48e-8 years
```

Worked example for AAPL (σ = 0.22, S = $190):

| Horizon | Ticks | σ of the log return | 1σ move in $ |
|---|---:|---:|---:|
| 1 tick (0.5 s) | 1 | 0.22·√8.48e-8 ≈ 0.0064% | ≈ $0.012 |
| 1 minute | 120 | 0.0064%·√120 ≈ 0.070% | ≈ $0.13 |
| 1 hour | 7,200 | ≈ 0.54% | ≈ $1.03 |
| 1 trading day (6.5 h) | 46,800 | ≈ 1.39% (= 0.22/√252) | ≈ $2.63 |

So prices tick by cents and drift by dollars over a session, which looks realistic. The drift term μ·dt ≈ 4e-9 per tick is invisible, as it should be. Because the update is exponential, prices can never go negative.

### 9.2 `GBMSimulator`

Keep the current implementation. The pieces that matter:

```python
class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for ticker in tickers:
            self._add_ticker_internal(ticker)   # batch add...
        self._rebuild_cholesky()                # ...then one factorization

    def step(self) -> dict[str, float]:
        n = len(self._tickers)
        if n == 0:
            return {}
        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z              # correlate the draws
        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)
            if random.random() < self._event_prob:          # §9.4
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock
            result[ticker] = round(self._prices[ticker], 2)
        return result
```

Internally the simulator keeps **unrounded** prices (`self._prices`) and only rounds what it returns. If it rounded internally, sub-cent moves would be thrown away every tick and prices would stick.

### 9.3 Correlation via Cholesky

To get correlated normals, build the correlation matrix C (n×n, ones on the diagonal, ρᵢⱼ from the sector rules), factor C = L·Lᵀ once, then for each tick compute `z_corr = L @ z_indep`. The result has `Cov(z_corr) = L·I·Lᵀ = C`.

Example with 3 tickers [AAPL, MSFT, JPM]:

```
C = [[1.0, 0.6, 0.3],          L = cholesky(C) =
     [0.6, 1.0, 0.3],              [[1.000, 0.000, 0.000],
     [0.3, 0.3, 1.0]]               [0.600, 0.800, 0.000],
                                    [0.300, 0.150, 0.942]]
```

Re-factoring costs O(n³) and happens only on add or remove (n ≤ ~50, well under 1 ms). `step()` costs O(n²) for the mat-vec, which is trivial.

### 9.4 Shock events

With p = 0.001 per ticker per tick, 10 tickers, and 2 ticks/s, the expected rate is 10 × 2 × 0.001 = **0.02 events/s, about one every 50 s** somewhere on the board. Each event is a ±2–5% jump applied on top of the GBM step. The cache records that tick's `direction` as up or down like any other, so the frontend flashes it. If you want these surfaced, log them at INFO; the demo (`market_data_demo.py`) already shows them.

### 9.5 `SimulatorDataSource` (R6 applied)

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator."""

    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5,
                 event_probability: float = 0.001) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        tickers = list(dict.fromkeys(t.strip().upper() for t in tickers))  # dedupe, keep order
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache so the first SSE frame and first trade have prices.
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)  # ref = seed = "session open"
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
        ticker = ticker.strip().upper()
        if self._sim and ticker not in self._sim.get_tickers():
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)  # priced immediately
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.strip().upper()
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")  # never let the loop die
            await asyncio.sleep(self._interval)
```

**There's no race between `step()` and `add_ticker()`.** Both run on the event loop and neither awaits mid-mutation, so the Cholesky matrix is always consistent with `_tickers` when `step()` runs. Keep it that way: never move `step()` into a thread.

---

## 10. Massive Client — `massive_client.py`

### 10.1 The API call

One request per poll, covering every tracked ticker:

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...
Authorization: Bearer $MASSIVE_API_KEY
```

```python
client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=["AAPL", "MSFT"])
# -> list[TickerSnapshot]
```

### 10.2 What the SDK actually returns (massive 2.2.0)

The fields below were checked against `massive/rest/models/snapshot.py` and `trades.py`. Some differ from the earlier archive notes:

```python
TickerSnapshot:
    ticker: str | None
    last_trade: LastTrade | None    # .price, .sip_timestamp (ns!), .size, .exchange
    last_quote: LastQuote | None
    min: MinuteSnapshot | None      # latest minute bar: .close, .timestamp (ms)
    day: Agg | None                 # today's bar: .open .high .low .close .volume
    prev_day: Agg | None            # yesterday's bar: .close = previous close
    todays_change: float | None
    todays_change_percent: float | None
    updated: int | None             # ns since epoch
```

Things to watch for:

- `LastTrade` has **no `timestamp` attribute**. Use `sip_timestamp`, which is in **nanoseconds**. This is bug R1.
- The previous close is `prev_day.close`. There is **no** `day.previous_close` on equity snapshots.
- Every field is `Optional`. Pre-market, or for thin tickers, `last_trade` or `day` may be `None`, and `day.close` can be `0` right after midnight ET, when the snapshot resets.

Example raw JSON for one ticker (the API's short keys):

```json
{
  "ticker": "AAPL",
  "todaysChange": 1.32, "todaysChangePerc": 0.69,
  "updated": 1760030400000000000,
  "day":     {"o": 190.1, "h": 192.4, "l": 189.7, "c": 191.9, "v": 41230000, "vw": 191.2},
  "prevDay": {"o": 189.0, "h": 191.0, "l": 188.2, "c": 190.58, "v": 52110000, "vw": 189.9},
  "min":     {"c": 191.88, "t": 1760030340000},
  "lastTrade": {"p": 191.9, "s": 100, "t": 1760030399123456789, "x": 4}
}
```

### 10.3 Parsing: `_extract_quote()` (R1, R2)

Take the first price that exists, from best to worst, and pull the reference price and timestamp from the same snapshot:

```python
def _extract_quote(snap) -> tuple[str, float, float, float | None] | None:
    """Return (ticker, price, timestamp_s, reference_price) or None if unusable.

    Price fallback: last trade -> latest minute close -> day close -> previous close.
    """
    ticker = getattr(snap, "ticker", None)
    if not ticker:
        return None

    lt, minute, day, prev = snap.last_trade, snap.min, snap.day, snap.prev_day
    candidates = (
        (lt.price if lt else None, lt.sip_timestamp / 1e9 if lt and lt.sip_timestamp else None),
        (minute.close if minute else None, minute.timestamp / 1e3 if minute and minute.timestamp else None),
        (day.close if day else None, None),
        (prev.close if prev else None, None),
    )
    for price, ts in candidates:
        if price:  # skips None and 0.0 (post-midnight reset)
            break
    else:
        return None

    if ts is None and snap.updated:
        ts = snap.updated / 1e9
    reference = prev.close if prev and prev.close else None
    return ticker.upper(), float(price), ts or time.time(), reference
```

With the JSON above, this returns `("AAPL", 191.9, 1760030399.123, 190.58)`, which gives `day_change_percent` = +0.69%. That matches `todaysChangePerc`.

### 10.4 Full module

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 120.0


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST snapshot API.

    Rate limits: free tier 5 req/min -> poll every 15s (default);
    paid tiers -> 2-5s via MASSIVE_POLL_INTERVAL.
    """

    def __init__(self, api_key: str, price_cache: PriceCache,
                 poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._consecutive_failures = 0

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(t.strip().upper() for t in tickers))
        await self._poll_once()  # cache is warm before the first SSE client connects
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(self._tickers), self._interval)

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
        ticker = ticker.strip().upper()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added %s (priced on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.strip().upper()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- internals ---

    def _next_delay(self) -> float:
        """Normal interval, doubled per consecutive failure, capped."""
        if self._consecutive_failures == 0:
            return self._interval
        return min(self._interval * 2 ** self._consecutive_failures, MAX_BACKOFF_SECONDS)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._next_delay())
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
        except Exception as e:  # AuthError, BadResponse (401/403/429), network
            self._consecutive_failures += 1
            logger.error("Massive poll failed (%d in a row): %s",
                         self._consecutive_failures, e)
            return
        self._consecutive_failures = 0

        processed = 0
        for snap in snapshots:
            try:
                quote = _extract_quote(snap)
            except (AttributeError, TypeError, ValueError) as e:
                logger.warning("Skipping snapshot for %s: %s",
                               getattr(snap, "ticker", "???"), e)
                continue
            if quote is None:
                continue
            ticker, price, ts, ref = quote
            if ticker not in self._tickers:  # removed while the request was in flight
                continue
            self._cache.update(ticker=ticker, price=price, timestamp=ts, reference_price=ref)
            processed += 1
        logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

    def _fetch_snapshots(self) -> list:
        """Synchronous SDK call; runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=list(self._tickers),  # copy: the list may change on the loop thread
        )


# _extract_quote() from §10.3 lives here at module level.
```

**Behavior notes**

- **Retries.** The SDK's urllib3 layer already retries 5xx/429 three times with backoff. Our backoff (15 → 30 → 60 → 120 s) sits on top of that, so we stay under rate limits when the key is bad or over quota.
- **A ticker removed mid-request** is dropped by the `ticker not in self._tickers` check. Without it, a late write would bring the evicted ticker back into the cache.
- **Unknown symbols** (e.g. `ZZZZ`) don't appear in the response at all. They're tracked but never priced. §15.1 explains how the watchlist API reports this.
- **When the market is closed**, every poll returns the same last price. The version still bumps and the SSE frame is resent with `direction: "flat"`. That's harmless, and it doubles as a heartbeat.

---

## 11. Factory — `factory.py`

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY non-empty -> MassiveDataSource, else SimulatorDataSource.

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = float(os.environ.get("MASSIVE_POLL_INTERVAL", "15") or 15)
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache,
                                 poll_interval=max(interval, 1.0))
    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

| Env | Result |
|---|---|
| `MASSIVE_API_KEY` unset, `""`, or `"   "` | `SimulatorDataSource` |
| `MASSIVE_API_KEY=abc` | `MassiveDataSource`, 15 s |
| `MASSIVE_API_KEY=abc MASSIVE_POLL_INTERVAL=3` | `MassiveDataSource`, 3 s (paid tier) |

---

## 12. SSE Streaming — `stream.py`

### 12.1 Wire format

Each frame is the **full snapshot** of all tracked tickers, keyed by ticker:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.42,"previous_price":190.38,"timestamp":1760030399.5,"change":0.04,"change_percent":0.021,"direction":"up","reference_price":190.0,"day_change":0.42,"day_change_percent":0.2211},"MSFT":{...}}

: ping

data: {...}
```

- `retry: 1000` is sent once at connect. It tells `EventSource` to reconnect 1 s after a drop.
- `data:` frames are sent at most every 0.5 s, and only when `cache.version` has changed.
- `: ping` is an SSE comment. `EventSource` ignores it; proxies see traffic. It's sent after 15 s with no data (R4).

**Why full snapshots and not one event per ticker?** At 10–50 tickers a frame is 2–10 KB, which is negligible over localhost. Full snapshots are also self-healing: a reconnecting client is complete after a single frame, and removals show up as missing keys. The frontend just replaces its map.

### 12.2 Module (R3, R4)

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

PUSH_INTERVAL = 0.5        # seconds between version checks / pushes
HEARTBEAT_INTERVAL = 15.0  # seconds of silence before a ": ping" comment


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build a fresh router bound to this cache (safe to call repeatedly)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = PUSH_INTERVAL,
    heartbeat: float = HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"

    last_version = -1
    idle = 0.0
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                data = {t: u.to_dict() for t, u in price_cache.get_all().items()}
                yield f"data: {json.dumps(data)}\n\n"  # {} is valid: empty watchlist
                idle = 0.0
            else:
                idle += interval
                if idle >= heartbeat:
                    yield ": ping\n\n"
                    idle = 0.0
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

Note that an **empty `{}` frame is sent** when the cache is empty, which the current code does not do. That way a client whose last ticker was removed sees the empty state.

### 12.3 Try it

```bash
curl -N http://localhost:8000/api/stream/prices
```

---

## 13. Ticker Tracking — `tracking.py`

### 13.1 The rule

```
tracked tickers  ==  watchlist tickers  ∪  tickers with an open position (quantity > 0)
```

The watchlist alone isn't enough. If a user removes NVDA from the watchlist while holding 5 shares, the portfolio value, positions table, heatmap, and 30-second snapshots all still need NVDA's price.

### 13.2 One idempotent reconcile function

Callers don't need to work out whether to add or remove anything. Each one recomputes the desired set and diffs it against what's tracked:

```python
"""Keep the market data source's ticker set in sync with the database."""

from __future__ import annotations

import logging
import sqlite3

from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def desired_tickers(conn: sqlite3.Connection, user_id: str = "default") -> list[str]:
    """Watchlist ∪ held positions, watchlist order first, then extra holdings."""
    watch = [r[0] for r in conn.execute(
        "SELECT ticker FROM watchlist WHERE user_id = ? ORDER BY added_at", (user_id,))]
    held = [r[0] for r in conn.execute(
        "SELECT ticker FROM positions WHERE user_id = ? AND quantity > 0 ORDER BY ticker",
        (user_id,))]
    return list(dict.fromkeys(watch + held))


async def sync_tracked_tickers(conn: sqlite3.Connection, source: MarketDataSource,
                               user_id: str = "default") -> tuple[set[str], set[str]]:
    """Reconcile the source with the DB. Returns (added, removed). Idempotent."""
    want = set(desired_tickers(conn, user_id))
    have = set(source.get_tickers())
    added, removed = want - have, have - want
    for t in sorted(added):
        await source.add_ticker(t)
    for t in sorted(removed):
        await source.remove_ticker(t)
    if added or removed:
        logger.info("Tracked tickers: +%s -%s", sorted(added), sorted(removed))
    return added, removed
```

### 13.3 When to call it

| Event | Effect |
|---|---|
| `POST /api/watchlist` (manual or LLM) | adds the ticker |
| `DELETE /api/watchlist/{t}` | removes it **unless held** |
| Trade that opens a position in an unwatched ticker | adds it |
| Sell that takes a position to 0 for an unwatched ticker | removes it |
| App startup | `start(desired_tickers(conn))`; no sync needed |

Call it **after** the DB transaction commits, so the set you compute reflects the change.

**Example sequence**

```
watchlist = [AAPL, NVDA], positions = {}          tracked = {AAPL, NVDA}
buy 5 NVDA                                        tracked = {AAPL, NVDA}       (no-op)
DELETE /api/watchlist/NVDA                        tracked = {AAPL, NVDA}       (held → kept)
sell 5 NVDA                                       tracked = {AAPL}             (removed)
buy 1 PYPL (not on watchlist)                     tracked = {AAPL, PYPL}       (added)
```

**Trading an untracked ticker.** A trade needs a price *before* it executes, but an untracked ticker has none. The trade route therefore calls `source.add_ticker(t)` *before* reading the price (§15.2). With the simulator, the price is available immediately. With Massive, it's available after the next poll, so the route returns 400 "price not yet available" and the user retries. If the trade then fails, the next `sync_tracked_tickers` removes the ticker again.

---

## 14. App Integration — `main.py`

### 14.1 Lifespan

```python
"""FinAlly FastAPI application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import get_connection, init_db          # owned by the DB/portfolio work
from app.market import PriceCache, create_market_data_source, create_stream_router
from app.market.tracking import desired_tickers

logging.basicConfig(level=logging.INFO)

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


# Module-level so the SSE router can be included at import time, before the
# static mount at "/" (Starlette matches routes in registration order).
price_cache = PriceCache()
market_source = create_market_data_source(price_cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()                                          # lazy schema + seed (PLAN §7)
    with get_connection() as conn:
        await market_source.start(desired_tickers(conn))

    # snapshot_task = asyncio.create_task(record_snapshots_forever(price_cache))  # §15.3
    try:
        yield
    finally:
        # snapshot_task.cancel()
        await market_source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)
app.state.price_cache = price_cache
app.state.market_source = market_source

# 1. API routes first
app.include_router(create_stream_router(price_cache))
# app.include_router(portfolio_router); app.include_router(watchlist_router); ...


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "tickers_priced": len(price_cache)}


# 2. Static frontend last: a mount at "/" would otherwise shadow /api/*
if STATIC_DIR.exists():  # absent in backend-only dev
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
```

> **Route order matters.** Starlette matches routes in the order they were registered, and a `StaticFiles` mount at `/` matches everything. All `/api` routers, including the SSE router, must be included **before** the mount. That's why the cache and the source are created at module scope and the lifespan only calls `start()`/`stop()`. `create_market_data_source()` reads the environment at import, so load `.env` (e.g. `docker --env-file`) before the app is imported.

### 14.2 Dependencies — `deps.py`

```python
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

Use `request.app` rather than importing the global `app`. That avoids circular imports, and tests can build their own app.

---

## 15. Consumers

### 15.1 Watchlist routes

```python
router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


@router.get("")
async def list_watchlist(cache: PriceCache = Depends(get_price_cache)):
    with get_connection() as conn:
        rows = conn.execute("SELECT ticker FROM watchlist WHERE user_id='default' "
                            "ORDER BY added_at").fetchall()
    out = []
    for (ticker,) in rows:
        u = cache.get(ticker)
        out.append({"ticker": ticker, **(u.to_dict() if u else {"price": None})})
    return out


@router.post("", status_code=201)
async def add_to_watchlist(body: WatchlistAdd,
                           cache: PriceCache = Depends(get_price_cache),
                           source: MarketDataSource = Depends(get_market_source)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(400, str(e))
    with get_connection() as conn:
        conn.execute("INSERT OR IGNORE INTO watchlist (id, user_id, ticker, added_at) "
                     "VALUES (?, 'default', ?, ?)", (str(uuid4()), ticker, now_iso()))
        conn.commit()
        await sync_tracked_tickers(conn, source)
    u = cache.get(ticker)   # simulator: priced now; Massive: None until next poll
    return {"ticker": ticker, "price": u.price if u else None}


@router.delete("/{ticker}")
async def remove_from_watchlist(ticker: str,
                                source: MarketDataSource = Depends(get_market_source)):
    ticker = ticker.strip().upper()
    with get_connection() as conn:
        cur = conn.execute("DELETE FROM watchlist WHERE user_id='default' AND ticker=?",
                           (ticker,))
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, f"{ticker} is not on the watchlist")
        await sync_tracked_tickers(conn, source)   # keeps it if held
    return {"ticker": ticker, "removed": True}
```

Example exchange:

```
POST /api/watchlist {"ticker":"pypl"}   → 201 {"ticker":"PYPL","price":143.27}
POST /api/watchlist {"ticker":"$$$"}    → 400 {"detail":"Invalid ticker symbol: '$$$'"}
DELETE /api/watchlist/PYPL              → 200 {"ticker":"PYPL","removed":true}
```

### 15.2 Trade execution: getting the fill price

```python
async def get_fill_price(ticker: str, cache: PriceCache, source: MarketDataSource) -> float:
    """Current price for a market order, or HTTP 400 if unavailable."""
    if ticker not in source.get_tickers():
        await source.add_ticker(ticker)          # simulator prices this immediately
    price = cache.get_price(ticker)
    if price is None:
        raise HTTPException(400, f"Price not yet available for {ticker}; try again shortly.")
    return price
```

After the trade commits, call `sync_tracked_tickers(conn, source)` (§13.3) and record a portfolio snapshot (§15.3). The LLM chat flow uses the **same** function, so an LLM trade fails with the same message, which then goes into the chat response (PLAN §9).

### 15.3 Portfolio valuation and snapshots

```python
def portfolio_value(conn, cache: PriceCache) -> dict:
    cash = conn.execute("SELECT cash_balance FROM users_profile WHERE id='default'").fetchone()[0]
    positions = []
    for ticker, qty, avg in conn.execute(
            "SELECT ticker, quantity, avg_cost FROM positions "
            "WHERE user_id='default' AND quantity > 0"):
        price = cache.get_price(ticker) or avg     # fall back to cost if unpriced
        pnl = (price - avg) * qty
        positions.append({"ticker": ticker, "quantity": qty, "avg_cost": avg,
                          "current_price": price, "market_value": price * qty,
                          "unrealized_pnl": round(pnl, 2),
                          "pnl_percent": round((price / avg - 1) * 100, 2) if avg else 0.0})
    total = cash + sum(p["market_value"] for p in positions)
    return {"cash_balance": cash, "total_value": round(total, 2), "positions": positions}


async def record_snapshots_forever(cache: PriceCache, interval: float = 30.0) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            with get_connection() as conn:
                v = portfolio_value(conn, cache)["total_value"]
                conn.execute("INSERT INTO portfolio_snapshots (id, user_id, total_value, "
                             "recorded_at) VALUES (?, 'default', ?, ?)",
                             (str(uuid4()), v, now_iso()))
                conn.commit()
        except Exception:
            logger.exception("Snapshot failed")
```

Falling back to `avg_cost` means a temporarily unpriced holding contributes zero P&L instead of zero value. Otherwise the P&L chart would show a fake crash.

### 15.4 LLM context

```python
def market_context(cache: PriceCache, tickers: list[str]) -> str:
    lines = []
    for t in tickers:
        u = cache.get(t)
        lines.append(f"{t}: ${u.price:.2f} ({u.day_change_percent:+.2f}% today)"
                     if u else f"{t}: price unavailable")
    return "\n".join(lines)
# AAPL: $190.42 (+0.22% today)
```

---

## 16. Frontend Contract

```ts
// types.ts — mirrors PriceUpdate.to_dict()
export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;          // Unix seconds (float)
  change: number;             // vs previous tick
  change_percent: number;     // vs previous tick
  direction: "up" | "down" | "flat";
  reference_price: number | null;
  day_change: number;         // vs reference (daily)
  day_change_percent: number; // use for the watchlist "Chg %" column
}
export type PriceSnapshot = Record<string, PriceUpdate>;
```

```ts
// usePriceStream.ts (sketch)
const es = new EventSource("/api/stream/prices");
es.onopen = () => setStatus("connected");
es.onerror = () =>
  setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
es.onmessage = (e) => {
  const snap: PriceSnapshot = JSON.parse(e.data);
  setPrices(snap);                                   // replace, don't merge (removals)
  for (const u of Object.values(snap)) {
    appendSparkline(u.ticker, u.timestamp, u.price); // dedupe on timestamp
    if (u.direction !== "flat") flash(u.ticker, u.direction);
  }
};
```

- **Dedupe sparkline points on `timestamp`.** In Massive mode the same frame is re-sent (§10.4 notes) with an unchanged timestamp.
- **Flash only on up or down**, and only when `timestamp` changed. That way a re-sent frame doesn't re-flash.
- The status dot maps to `onopen` (green), `onerror` with `CONNECTING` (yellow), and `CLOSED` (red).

---

## 17. Error Handling & Edge Cases

| Situation | Behavior |
|---|---|
| Empty watchlist at startup | Sources start with `[]`. The simulator steps nothing; Massive skips the request. SSE sends `{}`. |
| Malformed ticker from user or LLM | `normalize_ticker` raises → HTTP 400 / error in chat actions |
| Valid-looking but unknown ticker (Massive) | Tracked, never priced. `GET /api/watchlist` shows `price: null`. Trades return 400. |
| Unknown ticker (simulator) | Gets a random seed U(50, 300) and default params. Always priced. |
| Massive 401 (bad key) | Logged with backoff up to 120 s. No prices. `/api/health` shows `tickers_priced: 0`. Fix the key and restart. |
| Massive 429 | SDK retries ×3, then our exponential backoff |
| Ticker removed while a Massive request is in flight | Result discarded (the `ticker not in self._tickers` check) |
| Simulator step throws | Logged; the loop continues on the next tick |
| Client disconnects | `is_disconnected()` ends the generator, or the task is cancelled. Nothing leaks. |
| Idle proxy timeouts | `: ping` every 15 s |
| Trade on an untracked ticker | `get_fill_price` adds it. Sim: fills now; Massive: 400 until the next poll. |
| Removing a held ticker from the watchlist | Removed from the watchlist but still tracked (§13) |
| App restart | Simulator restarts from `SEED_PRICES` (prices don't persist; by design). Daily change resets to 0%. |

---

## 18. Testing

The existing 73 tests stay. Add or adjust the following.

### 18.1 Models and cache (R2, R5)

```python
def test_day_change_uses_reference():
    u = PriceUpdate("AAPL", 192.10, 192.05, 1.0, reference_price=190.0)
    assert u.day_change == 2.1
    assert u.day_change_percent == 1.1053
    assert u.to_dict()["day_change_percent"] == 1.1053

def test_reference_pinned_on_first_update_and_carried():
    c = PriceCache()
    c.update("AAPL", 190.0)
    c.update("AAPL", 191.0)
    assert c.get("AAPL").reference_price == 190.0

def test_explicit_reference_overrides():
    c = PriceCache()
    c.update("AAPL", 190.0, reference_price=185.0)
    assert c.get("AAPL").day_change == 5.0

def test_zero_timestamp_respected():
    c = PriceCache()
    assert c.update("AAPL", 1.0, timestamp=0.0).timestamp == 0.0

def test_remove_bumps_version():
    c = PriceCache(); c.update("AAPL", 1.0); v = c.version
    c.remove("AAPL"); assert c.version == v + 1
    c.remove("AAPL"); assert c.version == v + 1   # no-op doesn't bump
```

### 18.2 Simulator

```python
def test_full_default_set_cholesky():
    sim = GBMSimulator(list(SEED_PRICES))      # all 10 tickers
    assert sim._cholesky.shape == (10, 10)

def test_add_ticker_normalizes(cache):
    src = SimulatorDataSource(cache); await src.start(["AAPL"])
    await src.add_ticker(" aapl ")
    assert src.get_tickers() == ["AAPL"]
    await src.stop()

def test_tech_correlation_is_positive():
    np.random.seed(0)
    sim = GBMSimulator(["AAPL", "MSFT"], event_probability=0)
    a, m = [], []
    for _ in range(2000):
        p0 = (sim.get_price("AAPL"), sim.get_price("MSFT"))
        sim.step()
        a.append(math.log(sim.get_price("AAPL") / p0[0]))
        m.append(math.log(sim.get_price("MSFT") / p0[1]))
    assert 0.5 < np.corrcoef(a, m)[0, 1] < 0.7
```

### 18.3 SSE (R3, R4)

Drive the generator directly with a fake request. No server is needed.

```python
class FakeRequest:
    def __init__(self, polls: int):
        self._left = polls
        self.client = None
    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0

async def test_stream_emits_retry_then_data():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    frames = [f async for f in _generate_events(cache, FakeRequest(2), interval=0)]
    assert frames[0] == "retry: 1000\n\n"
    payload = json.loads(frames[1].removeprefix("data: "))
    assert payload["AAPL"]["price"] == 190.0
    assert len(frames) == 2                    # unchanged version → no second frame

async def test_heartbeat_when_idle():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    frames = [f async for f in _generate_events(cache, FakeRequest(4),
                                                interval=0.001, heartbeat=0.002)]
    assert ": ping\n\n" in frames

def test_router_factory_is_reentrant():
    r1 = create_stream_router(PriceCache()); r2 = create_stream_router(PriceCache())
    assert len(r1.routes) == 1 and len(r2.routes) == 1
```

### 18.4 Massive against real SDK models (R1)

This is the test that would have caught R1. Build snapshots with `TickerSnapshot.from_dict()` from the API's real JSON shape, not with `MagicMock`:

```python
from massive.rest.models import TickerSnapshot

RAW = {
    "ticker": "AAPL", "updated": 1760030400000000000,
    "day": {"o": 190.1, "c": 191.9}, "prevDay": {"c": 190.58},
    "min": {"c": 191.88, "t": 1760030340000},
    "lastTrade": {"p": 191.9, "s": 100, "t": 1760030399123456789},
}

def test_extract_quote_real_shape():
    t, price, ts, ref = _extract_quote(TickerSnapshot.from_dict(RAW))
    assert (t, price, ref) == ("AAPL", 191.9, 190.58)
    assert ts == pytest.approx(1760030399.123, abs=1e-3)

def test_extract_quote_falls_back_to_minute_then_prev():
    no_trade = {k: v for k, v in RAW.items() if k != "lastTrade"}
    assert _extract_quote(TickerSnapshot.from_dict(no_trade))[1] == 191.88
    only_prev = {"ticker": "AAPL", "prevDay": {"c": 190.58}, "day": {"c": 0}}
    assert _extract_quote(TickerSnapshot.from_dict(only_prev))[1] == 190.58

def test_extract_quote_unusable():
    assert _extract_quote(TickerSnapshot.from_dict({"ticker": "ZZZZ"})) is None

async def test_poll_writes_reference_price():
    cache = PriceCache()
    src = MassiveDataSource("k", cache); src._tickers = ["AAPL"]; src._client = MagicMock()
    with patch.object(src, "_fetch_snapshots", return_value=[TickerSnapshot.from_dict(RAW)]):
        await src._poll_once()
    assert cache.get("AAPL").day_change_percent == pytest.approx(0.69, abs=0.01)

async def test_backoff_grows_and_resets():
    src = MassiveDataSource("k", PriceCache(), poll_interval=15)
    src._tickers = ["AAPL"]; src._client = MagicMock()
    with patch.object(src, "_fetch_snapshots", side_effect=Exception("429")):
        await src._poll_once(); await src._poll_once()
    assert src._next_delay() == 60
    with patch.object(src, "_fetch_snapshots", return_value=[]):
        await src._poll_once()
    assert src._next_delay() == 15
```

### 18.5 Tracking (R7)

Use an in-memory SQLite DB with just the two tables:

```python
@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.executescript("""
      CREATE TABLE watchlist (ticker TEXT, user_id TEXT DEFAULT 'default', added_at TEXT);
      CREATE TABLE positions (ticker TEXT, user_id TEXT DEFAULT 'default', quantity REAL);
    """)
    return c

async def test_held_ticker_survives_watchlist_removal(conn):
    src = SimulatorDataSource(PriceCache()); await src.start(["AAPL", "NVDA"])
    conn.execute("INSERT INTO watchlist (ticker, added_at) VALUES ('AAPL', '1')")
    conn.execute("INSERT INTO positions (ticker, quantity) VALUES ('NVDA', 5)")
    assert await sync_tracked_tickers(conn, src) == (set(), set())
    conn.execute("UPDATE positions SET quantity = 0")
    assert await sync_tracked_tickers(conn, src) == (set(), {"NVDA"})
    await src.stop()
```

### 18.6 Manual check against the live API

```bash
cd backend
MASSIVE_API_KEY=... uv run python -c "
import asyncio; from app.market import PriceCache, create_market_data_source
async def m():
    c = PriceCache(); s = create_market_data_source(c); await s.start(['AAPL','MSFT'])
    print({t: u.to_dict() for t, u in c.get_all().items()}); await s.stop()
asyncio.run(m())"
```

You should see two priced tickers with a non-null `reference_price`. Before R1 is fixed, this prints `{}`.

---

## 19. Configuration Reference

| Setting | Where | Default | Notes |
|---|---|---|---|
| `MASSIVE_API_KEY` | env | empty | Non-empty → Massive |
| `MASSIVE_POLL_INTERVAL` | env | `15` | Seconds, min 1. Free tier needs ≥ 12. |
| `update_interval` | `SimulatorDataSource` | `0.5` s | Simulator tick |
| `event_probability` | `GBMSimulator` | `0.001` | Per ticker per tick |
| `dt` | `GBMSimulator` | `8.48e-8` | 0.5 s as a fraction of a trading year |
| `MAX_BACKOFF_SECONDS` | `massive_client.py` | `120` | Failure backoff cap |
| `PUSH_INTERVAL` | `stream.py` | `0.5` s | SSE version-check cadence |
| `HEARTBEAT_INTERVAL` | `stream.py` | `15` s | Idle `: ping` |
| `retry:` | `stream.py` | `1000` ms | Browser reconnect delay |

---

## 20. Implementation Checklist

Do these in order. Each step leaves the tests green.

1. **R5 + R2 (models/cache):** add `reference_price` and the day-change properties, fix `timestamp is None`, lock `version`, bump the version on `remove`. Add the §18.1 tests.
2. **R6:** add `normalize_ticker` to `interface.py`, normalize in `SimulatorDataSource`, and re-export it from `__init__.py`.
3. **R1 (massive):** add `_extract_quote`, pass `reference_price`, add the in-flight removal guard and backoff. Rewrite `_make_snapshot` in `test_massive.py` to build `TickerSnapshot.from_dict(...)`. `test_timestamp_conversion` and `test_malformed_snapshot_skipped` encode the old, wrong field names (`last_trade.timestamp` in ms) and fail against the fixed parser until they're rewritten. Then add the §18.4 tests.
4. **Factory:** read `MASSIVE_POLL_INTERVAL`.
5. **R3 + R4 (stream):** create the router in the factory, send `{}` frames, add the heartbeat. Add `tests/market/test_stream.py`.
6. **R7:** add `app/market/tracking.py` and `tests/market/test_tracking.py`.
7. **Integration:** write `app/main.py` and `app/deps.py` (mind the route order note in §14.1), and wire `sync_tracked_tickers` and `get_fill_price` into the watchlist, trade, and chat routes as those are built.
8. Run `uv run --extra dev pytest -v && uv run --extra dev ruff check app tests`, then `curl -N localhost:8000/api/stream/prices` to check the stream.
