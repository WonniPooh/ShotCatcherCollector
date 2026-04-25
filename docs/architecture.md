# Collector — Architecture

**Last Updated:** 2026-04-05
**Status:** Design / Pre-implementation

---

## Purpose

The Collector is a standalone Python process responsible for:

- Proactively discovering which symbols are (or recently were) traded and pre-loading their
  market data before anyone asks for it
- Maintaining a continuously refreshed local SQLite dataset (klines + aggregate trades) in
  `db_files/`
- Accepting on-demand load requests from the UI server via an internal WebSocket connection
- Streaming loading progress back to the UI server (which proxies it to the browser)

The system has three separate processes:

| Process | Responsibility | Binance keys? |
|---------|---------------|---------------|
| **Controller** | Manages C++ worker processes, strategy control, command dispatch | Yes (trading) |
| **Chart UI Server** | Serves chart frontend, proxies to Collector, reads SQLite | No |
| **Collector** | Downloads + refreshes market data, tracks active symbols | Yes (data) |

The Chart UI Server has **no Binance API keys** and never downloads market data.
The Collector has **no browser clients** and never serves HTTP to the frontend.
The Controller has **no charting or market data logic** — it only manages workers.

Collector ↔ Chart UI Server communicate exclusively through:

1. The **internal WS connection** (Collector runs a WS server; Chart UI Server connects as client)
2. The **shared `db_files/` directory** (Collector writes, Chart UI Server reads — SQLite WAL mode)

---

## Component Diagram

```
┌────────────────────────────────────────┐
│            Browser / Operator          │
└──────────────────┬─────────────────────┘
                   │ HTTPS / WSS
┌──────────────────▼─────────────────────┐
│         Chart UI Server Process        │
│         (Python / FastAPI)             │
│                                        │
│  ┌────────────────────────────────┐    │
│  │  CollectorClient               │    │
│  │  - Persistent WS to Collector  │◄───┼── ws://localhost:8001/ws
│  │  - Proxies load requests       │    │
│  │  - Forwards progress to /ws/ui │    │
│  └────────────────────────────────┘    │
│                                        │
│  ┌────────────────────────────────┐    │
│  │  market_data router            │    │
│  │  - Read-only SQLite queries    │    │
│  │  - Triggers load via           │    │
│  │    CollectorClient if missing  │    │
│  └────────────────────────────────┘    │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│           Collector Process            │
│                                        │
│  ┌─────────────────────────────────┐   │
│  │  WS Server  :8001/ws            │   │
│  │  - Accepts UI server connection │   │
│  │  - Dispatches load requests     │   │
│  │  - Emits progress events        │   │
│  └────────────────────────────────┘   │
│                                        │
│  ┌─────────────────────────────────┐   │
│  │  SymbolTracker                  │   │
│  │  - On startup: query position   │   │
│  │    history (7d) + open orders   │   │
│  │    via Binance REST → seed      │   │
│  │    initial watched set          │   │
│  │  - User data WS: real-time      │   │
│  │    position open/close events   │   │
│  │    → add/remove symbols         │   │
│  │  - Hourly account sync as       │   │
│  │    fallback for missed events   │   │
│  └─────────────────────────────────┘   │
│                                        │
│  ┌─────────────────────────────────┐   │
│  │  KlinesWorker (per symbol)      │   │
│  │  - Initial full load (7d of 1m) │   │
│  │  - Incremental refresh every N  │   │
│  │    minutes (gap-fill only)      │   │
│  └─────────────────────────────────┘   │
│                                        │
│  ┌─────────────────────────────────┐   │
│  │  TradesWorker (per symbol)      │   │
│  │  - Same pattern: initial full   │   │
│  │    load, then incremental       │   │
│  └─────────────────────────────────┘   │
│                                        │
│  ┌─────────────────────────────────┐   │
│  │  Binance API (aiohttp)          │   │
│  │  - Public REST: klines (no key) │   │
│  │  - Authenticated REST: account  │   │
│  │    position history, orders     │   │
│  │  - User data WS (listenKey)     │   │
│  └─────────────────────────────────┘   │
└──────────────────────┬─────────────────┘
                       │ SQLite WAL (write)
               ┌───────▼──────────┐
               │   db_files/      │
               │  SYMBOL/         │
               │   SYMBOL_1m.db   │
               │   trades.db      │
               │   trades_daily.db│
               └──────────────────┘
                       │ SQLite WAL (read)
        ┌──────────────┘
        │
┌───────▼──────────────────┐
│  Chart UI Server Process │ (read-only)
└──────────────────────────┘
```

---

## WS Protocol (Collector ↔ UI Server)

The Collector listens on `ws://localhost:8001/ws` (port configurable).  
The UI server maintains a single persistent connection as a client, reconnecting on drop.

### UI Server → Collector

| Type | Description | Fields |
|------|-------------|--------|
| `load` | Request data load for a symbol | `symbol` |
| `cancel` | Cancel in-progress load | `symbol` |
| `status` | Query current load state for a symbol | `symbol` |
| `list` | Get all currently watched symbols and their states | — |

### Collector → UI Server

| Type | Description | Fields |
|------|-------------|--------|
| `progress` | Loading progress update | `symbol`, `phase` (`klines`\|`trades`), `pct` (0–100) |
| `done` | Symbol data fully loaded/refreshed | `symbol`, `phase` |
| `error` | Load failed | `symbol`, `phase`, `msg` |
| `auto_loaded` | Symbol proactively loaded without explicit request | `symbol`, `reason` (`open_position`\|`position_history`\|`open_order`) |
| `status_response` | Response to `status` request | `symbol`, `state`, `pct`, `phase` |
| `list_response` | Response to `list` request | `symbols`: `[{symbol, state, pct, phase}]` |
| `watching` | Current full watchlist on connect | `symbols`: `[symbol]` |

The Chart UI Server proxies `progress`, `done`, `error`, and `auto_loaded` messages directly to
`/ws/ui` broadcast so browser clients receive live loading updates.

---

## Symbol Discovery Logic

```
On startup:
  1. Call GET /fapi/v1/positionRisk (7 days) → collect symbols with non-zero notional
  2. Call GET /fapi/v1/openOrders → collect symbols with open orders
  3. Union → initial watched set
  4. For each symbol in watched set: trigger KlinesWorker + TradesWorker

Runtime:
  1. User data WS receives ORDER_TRADE_UPDATE or ACCOUNT_UPDATE with new symbol
     → add to watched set, trigger workers
  2. Every 1h: call GET /fapi/v1/positionRisk again as fallback sync
     → add any newly seen symbols, prune symbols with no activity in >7 days
```

---

## Refresh Loop

Each watched symbol has a `KlinesWorker` and a `TradesWorker`.  
Both follow the same pattern:

```
initial_load():
  → Check latest candle/trade timestamp in DB
  → If gap > lookback_period: download full lookback_period
  → Else: download only the gap (incremental)

refresh_loop():
  → Every N minutes (default: 5m for klines, 1h for trades):
      → Compute gap from latest DB timestamp to now
      → If gap > 2m: download gap
```

Workers for the same symbol run concurrently (klines and trades in parallel).  
A symbol added at runtime gets the same `initial_load` → `refresh_loop` treatment.

---

## Planned File Structure

```
collector/
  main.py              — asyncio entry point, lifespan, WS server startup
  config.py            — settings: db_root, ws_port, API keys, lookback periods
  ws_server.py         — WS server, message dispatch, client registry
  symbol_tracker.py    — SymbolTracker: account REST + user data WS
  klines_worker.py     — KlinesWorker: download + refresh loop
  trades_worker.py     — TradesWorker: download + refresh loop
  binance_client.py    — thin async HTTP client (authenticated + public endpoints)
  requirements.txt
```

---

## Configuration

The collector reads from a config file (or env vars) at startup:

| Field | Description | Default |
|-------|-------------|---------|
| `db_root` | Path to `db_files/` directory | `../db_files` |
| `ws_port` | Internal WS server port | `8001` |
| `api_key` | Binance API key (for account REST + user data WS) | required |
| `api_secret` | Binance API secret | required |
| `klines_lookback_days` | How many days of klines to load on first access | `7` |
| `klines_refresh_interval_min` | Gap-fill frequency for klines | `5` |
| `trades_refresh_interval_min` | Gap-fill frequency for trades | `60` |
| `account_sync_interval_min` | Fallback account re-sync frequency | `60` |
| `symbol_inactive_prune_days` | Remove from watchlist after N days with no activity | `7` |

---

## Relationship to Controller

The Controller's `market_data.py` router becomes **read-only** after the Collector is
introduced:
- All `ensure_klines_loading` / `ensure_trades_loading` calls are replaced by a request to
  `CollectorClient.request_load(symbol)`
- `market_data_loader.py` and `trade_data_loader.py` are removed from `controller/`
- The controller no longer needs Binance HTTP access for market data

See [chart-ui-server/architecture.md](../chart-ui-server/architecture.md) for the Chart UI
Server component diagram, and [controller/architecture.md](../controller/architecture.md)
for the Controller (worker management) architecture.
