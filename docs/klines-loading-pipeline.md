# Klines Loading Pipeline — Design

## Problem

Binance S3 archives lag 1–2 days behind real-time. When a chart requests
"last 7 days of BTCUSDT 1m candles", we need:

1. **Bulk history** from archives (cheap, no rate limit)
2. **Gap fill** from REST API for the most recent 1–2 days
3. **Live stream** via WebSocket to stay current after initial load

All three sources write the same `CandleDB` (SQLite, `ON CONFLICT DO NOTHING`),
so overlaps are harmless and ordering doesn't matter for correctness.

## Architecture

```
klines_manager/
├── klines_db_manager.py              # CandleDB — single-symbol SQLite store
├── klines_archive_downloader.py      # S3 monthly/daily ZIP → CandleDB
├── klines_exchange_downloader.py     # REST /fapi/v1/klines gap-fill
├── klines_ws_stream.py               # WS @kline_1m live candle feed
└── klines_loader.py                  # Orchestrator — coordinates all sources
```

## Loading Flow (per symbol)

```
Request: load_symbol("ADAUSDT", lookback_days=7)

  ┌──────────────────────────────────────────────────────────┐
  │ 1. REST API: start from NOW, walk backwards              │
  │    - Provides fresh data immediately                     │
  │    - Each /fapi/v1/klines call: up to 1500 candles (25h) │
  │    - Stops when it reaches `lookback_start`              │
  │    - Emits progress to status_queue                      │
  ├──────────────────────────────────────────────────────────┤
  │ 2. Archive downloader: fill bulk history                  │
  │    - Downloads from `lookback_start` to latest archived  │
  │    - Uses S3 monthly + daily ZIPs (no rate limit)        │
  │    - Upserts — overlaps with REST data are harmless      │
  ├──────────────────────────────────────────────────────────┤
  │ 3. Gap verification + fill                                │
  │    - Scan DB for any remaining 1m gaps                   │
  │    - REST-fill any holes (rare — interrupted downloads)  │
  └──────────────────────────────────────────────────────────┘
```

## REST API Strategy

**Direction**: newest-first (start from `now`, walk backwards).

Rationale: the chart UI cares most about recent candles. By fetching backwards,
the newest data is available within seconds while the archive backfills.

**Endpoint**: `GET /fapi/v1/klines`
- Weight: 5 per call (shared IP limit with all other public endpoints)
- Max: 1500 candles per call (= 25 hours at 1m)
- Uses `binance_rate_limiter.bnx_limiter` for throttling

**Progress**: each successful API batch pushes to `status_queue`:
```python
{"symbol": "ADAUSDT", "source": "api", "phase": "loading",
 "pct": 60.0, "detail": "4320/7200 candles"}
{"symbol": "ADAUSDT", "source": "api", "phase": "done", "pct": 100.0}
```

## Idempotency

All three sources insert via `CandleDB.insert_rows()` which uses:
```sql
INSERT ... ON CONFLICT(open_time_ms) DO NOTHING
```

- Running the same load twice is safe (no duplicates)
- Overlapping time ranges between archive and REST are safe
- Out-of-order inserts are safe (primary key is `open_time_ms`)

## Resume / Restart Behaviour

| Scenario | Behaviour |
|----------|-----------|
| DB exists with recent data | REST starts from `MAX(open_time_ms)+1`, archive skips covered range |
| DB has gap > 3 days | Archive re-downloads from requested start (existing data preserved via upsert) |
| Requested start before existing start | Archive re-downloads wider range |
| Process crash mid-download | Temp CSVs survive on disk — archive resumes them on restart |

## Rate Limiting

Uses the shared `binance_rate_limiter.bnx_limiter`:
- Fixed 20s window, 750 weight/window (2250/min, 94% of 2400 hard cap)
- `KLINES_WEIGHT = 5` per REST call
- Backpressure at ≥2200 Binance-reported weight → 60s sleep
- Archive downloads hit S3 (no Binance rate limit)

## Gap Detection

After all sources complete, `klines_loader` scans the DB:
```sql
SELECT open_time_ms FROM candle
WHERE open_time_ms BETWEEN ? AND ?
ORDER BY open_time_ms
```
Expected: consecutive 60000ms intervals. Any gap > 60000ms is logged and
can be REST-filled if the gap falls within the API's 1000-day history limit.

## WebSocket Live Stream

`klines_ws_stream.py` — `KlineStream` class subscribes to
`wss://fstream.binance.com/market/stream?streams=<symbol>@kline_1m` and writes closed 1m candles
to the same `CandleDB`.

### Two-phase lifecycle

```
          ┌──────────────────────────────────────────────┐
          │ Phase 1: BUFFER mode (default on start)       │
          │  • WS events with k.x==true → in-memory list  │
          │  • DB untouched                                │
          ├──────────────────────────────────────────────┤
          │ flush_and_go_live()    ←── orchestrator calls  │
          ├──────────────────────────────────────────────┤
          │ Phase 2: LIVE mode                             │
          │  • Buffered candles bulk-inserted to DB        │
          │  • New closed candles written immediately      │
          └──────────────────────────────────────────────┘
```

**Why buffer first?** The orchestrator starts the WS stream *before* the
REST + archive load, so any candle that closes during the ~10–60s load phase
is captured. After the load finishes, `flush_and_go_live()` persists the buffer
and switches to direct writes — no gap between historical and live data.

### Connection management

- Uses `websockets` library (same as other WS clients in the project)
- Reconnects automatically on disconnect (5s default interval)
- `asyncio.CancelledError` → clean shutdown
- Only closed candles (`k.x == true`) are persisted; open-candle updates
  (pushed every 250ms) are ignored

### Event format (Binance → row)

```json
{
  "e": "kline", "s": "BTCUSDT",
  "k": {
    "t": 1638747660000, "T": 1638747719999,
    "o": "0.0010", "c": "0.0020", "h": "0.0025", "l": "0.0015",
    "v": "1000", "q": "1.0000", "n": 100,
    "V": "500", "Q": "0.500",
    "x": true
  }
}
```

Mapped via `_kline_event_to_row(k)` to the same dict format as REST/archive rows.

### API

```python
stream = KlineStream("ADAUSDT", db, on_candle=my_callback)
stream.start()                  # spawns background asyncio task
# ... initial load happens ...
stream.flush_and_go_live()      # bulk-write buffer, switch to live
# ... runs indefinitely ...
await stream.stop()             # cancel + close
```

Properties: `connected`, `live`, `candles_written`, `buffer_size`.
