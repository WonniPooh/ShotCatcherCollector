# Trades Collection

**Status:** ✅ IMPLEMENTED  
**Date:** 2025-04-01  
**Last Updated:** 2026-04-07  

---

## Overview

The trades collection subsystem gathers Binance Futures aggregate-trade data via three
complementary channels — **live WebSocket**, **REST gap-fill**, and **S3 daily archives** —
and stores them in per-symbol SQLite databases.  The design prioritizes *chart-first
latency*: live WS data reaches the UI instantly, REST fills the most recent ~24 h within
seconds, and archives backfill 5–7 days of history in the background.

All three channels feed through the same `AggTradeDB` with idempotent `ON CONFLICT DO NOTHING`
inserts, so overlapping data from any source is silently deduplicated.

## Architecture

```
              ┌─────────────┐
              │  main.py    │
              │ _run_trades │
              └──────┬──────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
  TradesWSStream  TradesLoader  TradesLoader
  (live aggTrade)  REST worker   Archive worker
        │            │            │
        │     ┌──────┴──────┐    │
        │     │  fill_gap() │    │  archive_process_symbol()
        │     │(backwards)  │    │  (S3 daily ZIPs → CSV → DB)
        │     └──────┬──────┘    │
        ▼            ▼           ▼
      ┌──────────────────────────────┐
      │  AggTradeDB  (per-symbol)    │
      │  trades.db ─ agg_trade table │
      └──────────────────────────────┘
```

**Key files:**

| Component | File |
|-----------|------|
| Orchestrator | `collector/main.py` |
| Loader + queue | `data_manager/trades_manager/trades_loader.py` |
| Live WS stream | `data_manager/trades_manager/trades_ws_stream.py` |
| REST downloader | `data_manager/trades_manager/trades_rest_downloader.py` |
| Archive downloader | `data_manager/trades_manager/trades_archive_downloader.py` |
| Database layer | `data_manager/trades_manager/trades_db_manager.py` |
| Status broadcast | `collector/ws_server.py` |

## How It Works

### 1. Entry Points

A symbol enters the trades pipeline in one of two ways:

| Trigger | Source | Priority | Behavior |
|---------|--------|----------|----------|
| Auto-discovery | `SymbolTracker` finds open position/order | Normal | Queued behind any active work |
| UI load request | Chart sends `LOAD <symbol>` | **High** | Preempts current REST download |

Both call `_trigger_symbol()` which creates a per-symbol `_run_trades` task (with
dedup — if a task is already running for that symbol, no new one is spawned).

### 2. _run_trades Sequence

Each `_run_trades(symbol)` coroutine executes these steps in order:

```
1. start_live(symbol)           ← subscribe to aggTrade WS
2. wait_for_first_trade(30s)    ← establish REST ↔ WS boundary
3. flush_live(symbol)           ← force immediate write of buffered trades
4. load_recent(days=7, end_ms=first_ts)   ← queue REST + archive work
5. await result
6. broadcast "done" to UI
```

The live WS stream starts **before** REST to guarantee zero gap: REST fills up to
`first_ts`, and WS covers everything from `first_ts` onward.

### 3. TradesLoader — Queue System

`TradesLoader` manages two persistent async workers:

| Worker | Queue | Processes |
|--------|-------|-----------|
| REST worker | `_rest_queue` | Recent gap-fills (`_do_rest`) |
| Archive worker | `_archive_queue` | Historical bulk downloads (`_do_archive`) |

Both workers run infinite loops pulling from their queue.  Each item is tracked
in `_active[symbol]` so it can be cancelled on preemption.

**Priority preemption:** When `priority=True`, the item is placed in a `_priority_rest`
slot and all currently active tasks are cancelled.  The REST worker checks the priority
slot first (with a 0.5 s timeout) so the high-priority item runs next.

### 4. Coverage Check — Skipping Redundant Downloads

Before queuing work, `load_recent()` checks whether the DB already covers the
requested range:

```python
if db_path.exists():
    max_ts = db.last_present_timestamp       # MAX(trade_ts_ms)
    if max_ts > archive_boundary_ms:         # recent data exists
        min_ts = SELECT MIN(trade_ts_ms)
        open_gaps = db.list_gaps()
        if min_ts <= start_ms and not open_gaps:
            return 0                          # fully covered, skip
```

This prevents re-downloading ~200 K+ trades on every restart or UI reload when the DB
already has complete 7-day coverage.

### 5. _do_rest — Three-Phase Execution

When a REST item is processed, `_do_rest()` runs three phases:

#### Phase 1: Recent REST Fill (fastest — UI-visible in seconds)

- Computes `rest_start_ms` = `end_ms - 1 day` (archives lag ~1 day on S3).
- Opens a **gap row** in `rest_gap` table: `(rest_start_ms, end_ms)`.
- Calls `fill_gap()` which walks **backwards** from `end_ms`:
  ```
  GET /fapi/v1/aggTrades?symbol=X&endTime=cursor-1&limit=1000
  ```
  Newest trades arrive first → chart renders quickly.
- Updates `frontier_ms` after each batch (crash-safe resumption).
- Closes gap row on completion.
- Emits `"done"` signal → UI can render chart.

#### Phase 2: Gap Resolution (interrupted previous runs)

- Queries `db.list_gaps()` for leftover gap rows from previous cancelled fills.
- Gaps entirely within Phase 1's range → closed without re-fetching.
- Partially overlapping gaps → frontier shrunk, only unfilled portion re-downloaded.
- Remaining gaps → filled via REST or archive depending on position.

#### Phase 3: Archive Backfill (slow — background)

- If the requested range extends beyond yesterday:
  - Calls `archive_process_symbol()` for S3 bulk download.
  - 16-connection concurrency, 64-connection TCP pool.
  - Downloads daily ZIP files → extracts CSV → inserts to DB.
  - Emits `"archive-phase"` status.

```
Timeline:
|← 7 days ago ─── Phase 3 (archive) ───|── Phase 1 (REST) ──|── WS live ──→|
                                    yesterday              first_ts        now
```

### 6. Live WebSocket Stream

`TradesWSStream` manages one or more Binance combined WS connections
(`wss://fstream.binance.com/market/stream?streams=...@aggTrade`), each handling up
to **10 symbols**.

| Aspect | Detail |
|--------|--------|
| Buffering | Trades accumulate in per-symbol lists |
| Flush interval | Every **10 s** (configurable `FLUSH_INTERVAL_S`) |
| First-trade sync | `_first_trade_events[symbol]` asyncio.Event set on first arrival |
| Force flush | `flush_live(symbol)` swaps buffer and inserts immediately |
| Subscription expiry | `_subscriptions[symbol] = until_ms`; swept every 30 min |
| Dynamic sub/unsub | `SUBSCRIBE`/`UNSUBSCRIBE` JSON frames on existing connection |
| Reconnect | 5 s backoff on `ConnectionClosed`, worker stays alive |

### 7. REST Downloader — fill_gap()

`fill_gap()` is the core REST fetcher.  It walks **backwards** from `end_ms` to
`start_ms`, fetching 1000 trades per batch.

```
Batch 1:  ←────── cursor = end_ms
Batch 2:  ←────── cursor = oldest_ts from batch 1
  ...
Batch N:  ←────── cursor ≤ start_ms → done
```

Each batch:
1. `GET /fapi/v1/aggTrades?symbol=X&endTime=cursor-1&limit=1000` (weight: 20)
2. Parse JSON → row tuples.
3. `db.insert_rows(rows)` (idempotent).
4. `db.update_gap(gap_id, oldest_ts)` — persistent frontier.
5. Log progress every 10 s.
6. `rate_limiter.check_backpressure()` — yield if near weight cap.

On `CancelledError`, gap row persists with last `frontier_ms` → next run resumes there.

### 8. Archive Downloader

`archive_process_symbol()` handles bulk historical data from Binance's public S3 bucket.

**Steps:**
1. **Resume leftover CSVs** — checks `temp_dir/*.csv` from interrupted previous run.
2. **Resolve start time** — queries DB `MIN/MAX(trade_ts_ms)` to advance past existing data.
3. **Build work plan** — lists monthly + daily ZIPs from S3; prefers monthly for ranges ≥ 8 days.
4. **Download** — concurrent fetch (10 per asset), extract ZIP → CSV to temp dir.
5. **Insert** — read CSV, parse rows, batch-insert to DB, delete temp file.
6. **Cleanup** — remove temp dir.

S3 endpoints:
```
https://data.binance.vision/data/futures/um/daily/aggTrades/<SYMBOL>/
https://data.binance.vision/data/futures/um/monthly/aggTrades/<SYMBOL>/
```

### 9. Database Layer — AggTradeDB

Per-symbol SQLite DB at `<db_root>/<SYMBOL>/trades.db`.

**Schema:**
```sql
CREATE TABLE agg_trade (
    agg_trade_id    INTEGER PRIMARY KEY,   -- Binance aggTradeId
    trade_ts_ms     INTEGER NOT NULL,
    price           REAL NOT NULL,
    qty             REAL NOT NULL,
    is_buyer_maker  INTEGER NOT NULL,      -- 0 or 1
    trades_num      INTEGER NOT NULL       -- ≥ 1
);
CREATE INDEX idx_time ON agg_trade(trade_ts_ms);
CREATE INDEX idx_side_time ON agg_trade(is_buyer_maker, trade_ts_ms);

CREATE TABLE rest_gap (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    gap_start_ms  INTEGER NOT NULL,        -- oldest timestamp desired
    frontier_ms   INTEGER NOT NULL         -- oldest ts reached so far
);

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
```

**Deduplication:** `INSERT ... ON CONFLICT(agg_trade_id) DO NOTHING` — all three data
sources (WS, REST, archive) can safely overlap.

**Pragmas:** WAL journal mode, normal sync, 256 MiB mmap for read-heavy scans.

**Timestamp normalization:** Accepts ms or µs (S3 dumps sometimes use µs); normalizes
to ms in `_coerce_row()`.

### 10. Progress Reporting

```
TradesLoader._emit()  →  _status_queue  →  _consume_status_queue()  →  ws_server.broadcast()
                                                                            ↓
                                                                     chart-ui-server
```

| Phase | UI message type | Progress |
|-------|----------------|----------|
| REST download | `progress` | 0–100% (span-based) |
| Archive download | `progress / archive-phase` | 0–50% download, 50–100% ingest |
| Completion | `done` | `trades: N trades` |

## Corner Cases & Error Handling

### Case: Restart with Existing Data
- **Trigger:** Collector restarts after previously completing a full 7-day load.
- **Behavior:** `load_recent()` checks `MIN/MAX(trade_ts_ms)` + `list_gaps()`.  If
  `min_ts ≤ start_ms` and no open gaps exist, returns 0 immediately — no REST calls.
- **Rationale:** Avoids re-downloading 200 K+ trades (wastes bandwidth + rate limit weight).

### Case: Restart with Interrupted REST Fill
- **Trigger:** Collector was killed mid-download (SIGTERM, crash, UI symbol switch).
- **Behavior:** Gap row persists in `rest_gap` with last `frontier_ms`.  Next run's
  Phase 2 detects it and fills only the remaining `[gap_start_ms, frontier_ms)` range.
- **Rationale:** Picking up from the frontier avoids re-downloading batches already in DB.

### Case: Restart with Interrupted Archive Download
- **Trigger:** Killed while downloading/ingesting S3 ZIPs.
- **Behavior:** Extracted CSVs remain in `temp_dir/`.  Next run detects and ingests them
  before starting new downloads.  `_resolve_start_time()` then skips days already present.
- **Rationale:** No wasted S3 bandwidth for already-extracted data.

### Case: UI Symbol Switch Mid-Download
- **Trigger:** User clicks a different symbol in the chart while REST fill is in progress.
- **Behavior:** `_on_ui_load_request()` calls `_trades_loader.cancel(old_symbol)`.  The
  REST worker's `CancelledError` handler preserves the gap row.  New symbol is queued
  with `priority=True`, preempting the REST worker immediately.
- **Rationale:** Chart responsiveness > background completeness.

### Case: Duplicate Trigger for Same Symbol
- **Trigger:** Multiple LOAD requests for the same symbol (scroll events, reconnects).
- **Behavior:** Two layers of dedup:
  1. `_trigger_symbol()` checks `_trades_tasks[symbol].done()` — won't spawn a second task.
  2. `_on_ui_load_request()` checks `_ui_load_symbol == symbol` — skips entirely.
- **Rationale:** Prevents duplicate REST downloads and wasted bandwidth.

### Case: WS First Trade Timeout
- **Trigger:** No trades arrive within 30 s (very low-volume symbol).
- **Behavior:** `wait_for_first_trade()` returns `None`.  `_run_trades` falls back to
  `end_ms = now + 5s` (small buffer), so REST covers up to the present.
- **Rationale:** Low-volume symbols may not trade for minutes; the 5 s buffer ensures
  overlap between REST and whenever the first WS trade eventually arrives.

### Case: Transient Network Error During REST
- **Trigger:** SSL reset, HTTP 5xx, timeout during `fill_gap()`.
- **Behavior:** `break` out of batch loop.  Gap row retains last frontier.  Next
  `load_recent()` or gap resolution pass resumes from that point.
- **Rationale:** No data loss; idempotent inserts handle any overlap on retry.

### Case: Rate Limit Backpressure
- **Trigger:** Binance `X-MBX-USED-WEIGHT-1M` approaches limit.
- **Behavior:** `rate_limiter.check_backpressure()` yields (async sleep) until weight
  recovers.  REST batch loop pauses without error.
- **Rationale:** Prevents 429 errors and potential IP bans.

## Configuration

| Field | Source | Default | Description |
|-------|--------|---------|-------------|
| `db_root` | `CollectorConfig` | `"db_files"` | Root directory for per-symbol DBs |
| `CollectorConfig` | `False` | Use WS/REST endpoints |
| `days` | Hardcoded in `_run_trades` | `7` | Lookback window for `load_recent()` |
| `archive_boundary` | Hardcoded | `now - 2 days` | S3 lag buffer: REST covers last 2 days, archives go older |
| `FLUSH_INTERVAL_S` | Hardcoded | `10.0` | WS buffer flush period |
| `MAX_SYMBOLS_PER_CONN` | Hardcoded | `10` | Symbols per WS combined stream |
| `EXPIRY_CHECK_S` | Hardcoded | `1800.0` | WS subscription expiry sweep (30 min) |

DB path layout: `<db_root>/<SYMBOL>/trades.db`

## Testing

### Integration Tests

| Test | File | What it verifies |
|------|------|-----------------|
| REST fill | `data_manager/trades_manager/tests/test_trades_rest_integration.py` | Backwards walk, gap tracking, dedup, coverage bounds |
| Loader full flow | `data_manager/trades_manager/tests/test_trades_loader_integration.py` | load_recent 7 days, resume, priority preemption |
| Archive download | `data_manager/trades_manager/tests/test_trades_archive_integration.py` | S3 daily/monthly ZIP fetch + ingest |
| WS stream | `data_manager/trades_manager/tests/test_trades_ws_integration.py` | Live aggTrade subscribe, buffer, flush |
| Cleanup | `data_manager/trades_manager/tests/test_trades_cleanup_integration.py` | Old data pruning |

All integration tests require live Binance mainnet and are run manually.

## Related

- [configuration.md](../configuration.md) — Config fields
- [rate-limiter.md](rate-limiter.md) — Rate limiting for REST calls
- [architecture.md](../design/architecture.md) — System architecture
- [rest-websocket-client.md](rest-websocket-client.md) — REST/WS client layer
