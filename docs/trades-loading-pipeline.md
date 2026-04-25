# Trades Loading Pipeline — Design

## Problem

The trades pipeline is more complex than klines because:
- **Volume**: aggTrades arrive much faster than 1m candles (~100–1000/s for active pairs)
- **Multi-symbol**: must handle 20+ symbols concurrently on live WS
- **Lifecycle**: WS streams have timed activity — collect for N hours, extendable
- **Two DB tiers**: common DB (rolling 7-day window, cleaned daily) + historical DBs (on-demand, separate per request)
- **Cancellation**: in-flight loads must be cancellable when symbol changes

## Architecture

```
trades_manager/
├── trades_db_manager.py              # AggTradeDB — per-symbol SQLite store (unchanged)
├── trades_archive_downloader.py      # S3 monthly/daily ZIP → trades.db (refactored)
├── trades_rest_downloader.py         # REST /fapi/v1/aggTrades gap-fill (new)
├── trades_ws_stream.py               # WS aggTrade multi-symbol live feed (new)
├── trades_loader.py                  # Orchestrator — queues, progress, lifecycle (new)
└── trades_cleanup.py                 # Daily stale-data cleanup for common DB (new)
```

## DB Layout (per symbol)

```
db_files/<SYMBOL>/
├── trades.db                    # "Common DB" — rolling 7-day window of aggTrades
│                                  - Fed by WS stream (live) + REST gap-fill
│                                  - Cleaned daily: rows older than 7 days deleted
│
├── trades_hist_<start>-<end>.db  # "Historical DB" — on-demand archive loads
│                                  - Date range is inclusive [start, end]
│                                  - Filename format: DDMMYY (e.g. trades_hist_050226-100226.db)
│                                  - Created per load request
│                                  - Auto-cleaned if unused for 3 days
│                                  - If request < 8 days → daily archives only
│                                  - If request ≥ 8 days → monthly + daily archives
```

## Component Details

### 1. `TradesWSStream` — Multi-symbol live aggTrade feed

**Key difference from KlineStream**: handles 20+ symbols across **multiple WS
connections**, max 10 symbols per connection.  When the 11th symbol subscribes,
a second connection is spawned.  Each connection uses Binance's combined stream:
`wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/...`

```
┌─────────────────────────────────────────────────────────┐
│ TradesWSStream (manager)                                 │
│                                                         │
│  subscribe("BTCUSDT", until=now+1h)                     │
│  subscribe("ETHUSDT", until=now+2h)                     │
│  extend("BTCUSDT", until=now+3h)                        │
│  unsubscribe("ETHUSDT")                                 │
│                                                         │
│  ┌──────────────────┐  ┌──────────────────┐              │
│  │ _Connection #1   │  │ _Connection #2   │              │
│  │ symbols 1–10     │  │ symbols 11–20    │              │
│  │ wss://...stream  │  │ wss://...stream  │              │
│  └──────────────────┘  └──────────────────┘              │
│                                                         │
│  Internal:                                               │
│   - _subscriptions: {symbol: expiry_ms}                  │
│   - _connections: list[_Connection]                       │
│   - _buffers: {symbol: [rows...]}  (shared across conns) │
│   - Flush timer: every 10s, bulk-insert per-symbol       │
│   - Expiry timer: every 30min, check & remove expired     │
│   - Each _Connection reconnects independently            │
│   - New symbol → pick connection with <10 symbols,       │
│     or spawn new _Connection                             │
└─────────────────────────────────────────────────────────┘
```

**Flush strategy**: every 10 seconds, for each symbol with buffered trades:
  1. `db.insert_rows(buffer)` — bulk insert
  2. Clear buffer

**Expiry**: each subscription has an `until_ms` timestamp.
  - `subscribe(symbol, until_ms)` — adds/updates subscription
  - `extend(symbol, until_ms)` — updates expiry (must be > current)
  - `unsubscribe(symbol)` — immediate removal, flush remaining buffer
  - Timer every 30 minutes sweeps expired subscriptions

**Connection management**: max 10 symbols per WS connection.  Each
`_Connection` uses the combined stream endpoint and sends
SUBSCRIBE/UNSUBSCRIBE via the live connection.  When a connection empties
(all its symbols unsubscribed/expired), it is closed and removed.

### 2. `TradesRestDownloader` — REST gap-fill

Similar to klines_exchange_downloader but for `/fapi/v1/aggTrades`:
- Weight: 20 per call
- Max: 1000 trades per call
- Walks backwards from `end_ms` to `start_ms`
- Uses shared `bnx_limiter`
- Cancellation via `asyncio.CancelledError`

`load_recent(days=7)` uses **archive for bulk + REST for the gap**:
1. Daily archive files are downloaded for days 3–N (archives lag ~1–2 days)
2. REST gap-fill covers only the remaining last ~2 days
3. This avoids excessive REST weight usage for large ranges

### 3. `TradesLoader` — Orchestrator with queues

Two separate queues:

```
┌─────────────────────────────────────────────────────────┐
│  REST Queue (sequential)                                 │
│  ┌──────┐ ┌──────┐ ┌──────┐                             │
│  │ sym1 │→│ sym2 │→│ sym3 │  ← processed one at a time  │
│  └──────┘ └──────┘ └──────┘                             │
│                                                         │
│  Archive Queue (sequential per-request, parallel inside) │
│  ┌──────────┐ ┌──────────┐                               │
│  │ req1     │→│ req2     │  ← one request at a time      │
│  │ (60d BTC)│ │ (30d ETH)│    but files download in      │
│  └──────────┘ └──────────┘    parallel within request    │
└─────────────────────────────────────────────────────────┘
```

**Public API:**

```python
loader = TradesLoader(db_root, rate_limiter, status_queue)
loader.start()

# Common DB: REST gap-fill for recent data (queued)
await loader.load_recent(symbol, days=7)

# Historical: archive load into separate DB (queued)
# Returns DB filename, e.g. "trades_hist_050226-100226.db"
db_name = await loader.load_historical(symbol, start_dt, end_dt)

# Cancel any in-flight load for symbol
loader.cancel(symbol)

# WS live stream management
loader.start_live(symbol, until_ms=now+3600*1000)
loader.extend_live(symbol, until_ms=now+7200*1000)
loader.stop_live(symbol)

await loader.stop()
```

**Progress reporting** via `status_queue`:
```python
# Queue position
{"symbol": "ETHUSDT", "type": "queued", "queue": "rest", "position": 2, "queue_size": 3}

# Active download progress
{"symbol": "BTCUSDT", "type": "progress", "source": "rest", "pct": 45.0, "detail": "4500/10000 trades"}
{"symbol": "BTCUSDT", "type": "progress", "source": "archive", "pct": 60.0, "detail": "3/5 files"}

# Completion
{"symbol": "BTCUSDT", "type": "done", "source": "rest", "trades": 10000}

# Cancellation
{"symbol": "ETHUSDT", "type": "cancelled"}
```

### 4. `TradesCleanup` — Stale-data removal

Two cleanup tasks:

**Common DB** — runs once per day (or on demand):
```python
cleanup_common_db(db_root, symbol, max_age_days=7)
```
Deletes rows from `trades.db` where `trade_ts_ms < now - 7 days`.

**Historical DBs** — runs once per day:
```python
cleanup_stale_historical_dbs(db_root, symbol, unused_days=3)
```
Deletes `trades_hist_*.db` files whose `mtime` is older than 3 days
(i.e. nobody read or wrote them for 3 days).

### 5. Cancellation

Every long-running async operation checks `asyncio.CancelledError`:
- REST downloader: between batches
- Archive downloader: between file downloads
- `TradesLoader.cancel(symbol)` cancels the active task for that symbol

## WS aggTrade Event Format

```json
{
  "e": "aggTrade",
  "E": 1672515782136,
  "s": "BTCUSDT",
  "a": 164032,
  "p": "0.001",
  "q": "100",
  "f": 300,
  "l": 300,
  "T": 1672515782136,
  "m": true
}
```

Mapped to AggTradeDB row:
```python
(a, T, float(p), float(q), 1 if m else 0, l - f + 1)
```

## Combined Stream WS Protocol

Endpoint: `wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade`

**Dynamic subscription** via WS message:
```json
{"method": "SUBSCRIBE", "params": ["adausdt@aggTrade"], "id": 1}
{"method": "UNSUBSCRIBE", "params": ["adausdt@aggTrade"], "id": 2}
```

Response format for combined stream wraps each event:
```json
{"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", ...}}
```

## Implementation Phases

### Phase 1: Core components
- `trades_rest_downloader.py` — REST gap-fill with cancellation
- `trades_ws_stream.py` — Multi-symbol WS with subscribe/unsubscribe, 10s flush, expiry
- `trades_cleanup.py` — Simple stale-data cleaner

### Phase 2: Orchestrator
- `trades_loader.py` — Queues (REST + archive), progress reporting, cancellation
- Refactor `trades_archive_downloader.py` — logger, status_queue, same cleanup as klines version

### Phase 3: Integration + tests
- WS integration test (receive real trades for BTCUSDT, verify flush to DB)
- Archive integration test (download 60 days, verify data)
- Orchestrator integration test (full pipeline: REST + archive + WS)
- Update collector/main.py to use new TradesLoader
