"""
SymbolTracker — discovers and maintains the set of symbols to collect data for.

Sources:
  1. On startup: query Binance REST for open orders
  2. Runtime: Binance User Data WS events (ORDER_TRADE_UPDATE, ACCOUNT_UPDATE)
  3. Hourly fallback: re-query REST to catch any missed WS events

Calls on_symbol_added(symbol) when a new symbol enters the watch set.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Callable, Set

logger = logging.getLogger("collector.symbol_tracker")

# How far back to look in order history on startup (ms)
_24H_MS = 24 * 60 * 60 * 1000


class SymbolTracker:
    def __init__(
        self,
        rest_client,  # BinanceFuturesClient
        on_symbol_added: Callable[[str], None],
        account_sync_interval_min: int = 60,
        symbol_inactive_prune_days: int = 7,
        db_root: str = "",
    ):
        self._client = rest_client
        self._on_symbol_added = on_symbol_added
        self._sync_interval_s = account_sync_interval_min * 60
        self._prune_threshold_ms = symbol_inactive_prune_days * 24 * 60 * 60 * 1000
        self._db_root = db_root

        self._watched: Set[str] = set()
        # Maps symbol → last_seen_ms (epoch ms of last known activity)
        self._last_seen: dict[str, int] = {}

        self._sync_task: asyncio.Task | None = None

    @property
    def watched_symbols(self) -> frozenset[str]:
        return frozenset(self._watched)

    async def start(self) -> None:
        """Run initial REST scan then start periodic sync loop."""
        await self._initial_scan()
        self._sync_task = asyncio.create_task(
            self._periodic_sync(), name="symbol-tracker-sync"
        )

    async def stop(self) -> None:
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

    # ── WS event callback (called by UserDataWS) ──────────────────────────────

    def on_order_event(self, event: dict) -> None:
        """Handle ORDER_TRADE_UPDATE from user data WS."""
        # event is the parsed flat dict from UserDataWS._handle_order_trade_update;
        # symbol is at the top-level "s" key.
        symbol = event.get("s")
        if symbol:
            self._touch(symbol)

    def on_account_update(self, event: dict) -> None:
        """Handle ACCOUNT_UPDATE (position changes) from user data WS."""
        # event is the parsed dict from UserDataWS._handle_account_update;
        # positions are at the top-level "positions" key.
        for pos in event.get("positions", []):
            symbol = pos.get("s")
            if symbol and float(pos.get("pa", 0)) != 0:
                self._touch(symbol)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _touch(self, symbol: str) -> None:
        self._last_seen[symbol] = int(time.time() * 1000)
        if symbol not in self._watched:
            logger.info("New symbol added to watch set: %s", symbol)
            self._watched.add(symbol)
            self._on_symbol_added(symbol)

    async def _initial_scan(self) -> None:
        """Discover all symbols from Binance account.

        Sources (union of all):
          1. Open orders
          2. All account positions (any with non-zero amount or unrealized PnL)
          3. Income history (realized PnL, commissions, funding) — discovers
             every symbol traded within the configured lookback window
        """
        logger.info("Running initial symbol scan...")
        symbols: set[str] = set()

        try:
            open_orders = await self._client.get_open_orders()
            for o in open_orders:
                symbols.add(o["symbol"])
            logger.info("Open orders: %d symbols", len(symbols))
        except Exception as exc:
            logger.warning("Failed to fetch open orders: %s", exc)

        try:
            account = await self._client.get_account()
            acct_syms: set[str] = set()
            for p in account.get("positions", []):
                sym = p.get("symbol", "")
                if not sym:
                    continue
                has_position = float(p.get("positionAmt", 0)) != 0
                has_pnl = float(p.get("unrealizedProfit", 0)) != 0
                if has_position or has_pnl:
                    acct_syms.add(sym)
            symbols.update(acct_syms)
            logger.info("Account positions: %d symbols", len(acct_syms))
        except Exception as exc:
            logger.warning("Failed to fetch account positions: %s", exc)

        # Income history — paginate through all records in the lookback window
        try:
            income_syms = await self._scan_income_history()
            symbols.update(income_syms)
            logger.info("Income history: %d symbols", len(income_syms))
        except Exception as exc:
            logger.warning("Failed to fetch income history: %s", exc)

        logger.info("Initial scan found %d symbols total: %s", len(symbols), sorted(symbols))
        for sym in symbols:
            self._touch(sym)

    async def _scan_income_history(self) -> set[str]:
        """Paginate through /fapi/v1/income to find all traded symbols.

        Goes back up to `symbol_inactive_prune_days` (from config) or 89 days
        (Binance max), whichever is smaller.
        """
        symbols: set[str] = set()
        now_ms = int(time.time() * 1000)
        max_lookback = 89 * 24 * 60 * 60 * 1000
        lookback = min(self._prune_threshold_ms, max_lookback)
        cursor = now_ms - lookback

        while cursor < now_ms:
            records = await self._client.get_income_history(
                start_time=cursor, end_time=now_ms, limit=1000
            )
            if not records:
                break
            for r in records:
                sym = r.get("symbol", "")
                if sym:
                    symbols.add(sym)
            # Move cursor past the last record
            last_time = max(int(r.get("time", 0)) for r in records)
            if last_time <= cursor:
                break  # no progress, avoid infinite loop
            cursor = last_time + 1

        return symbols

    async def _periodic_sync(self) -> None:
        """Hourly fallback: re-scan REST to catch any events missed by WS."""
        while True:
            await asyncio.sleep(self._sync_interval_s)
            try:
                await self._initial_scan()
                self._prune_inactive()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("Periodic sync failed: %s", exc)

    def _prune_inactive(self) -> None:
        """Remove symbols with no activity for longer than the prune threshold.

        Also checks: if a symbol still has open orders or recent DB activity,
        keep it even if _last_seen is stale (covers WS gaps).
        """
        now_ms = int(time.time() * 1000)
        # Symbols with recent local DB activity
        recent_db: set[str] = set()
        if self._db_root:
            try:
                recent_db = self._scan_local_db_for_recent(now_ms - _24H_MS)
            except Exception:
                pass  # best-effort; if scan fails, prune normally

        to_prune = []
        for sym, last in self._last_seen.items():
            if (now_ms - last) > self._prune_threshold_ms and sym not in recent_db:
                to_prune.append(sym)

        for sym in to_prune:
            logger.info("Pruning inactive symbol: %s (last seen %dh ago)",
                        sym, (now_ms - self._last_seen[sym]) // 3_600_000)
            self._watched.discard(sym)
            del self._last_seen[sym]

    def _scan_local_db_for_recent(self, cutoff_ms: int) -> set[str]:
        """Scan local per-symbol DB directories for recent order activity.

        Looks at each <db_root>/<SYMBOL>/order_events.db and checks if
        there are order events newer than cutoff_ms.
        """
        import sqlite3

        symbols: set[str] = set()
        if not self._db_root or not os.path.isdir(self._db_root):
            return symbols

        for name in os.listdir(self._db_root):
            sym_dir = os.path.join(self._db_root, name)
            db_path = os.path.join(sym_dir, "order_events.db")
            if not os.path.isfile(db_path):
                continue
            try:
                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT MAX(transaction_time_ms) FROM order_event"
                ).fetchone()
                conn.close()
                if row and row[0] and row[0] >= cutoff_ms:
                    symbols.add(name)
            except Exception:
                continue

        if symbols:
            logger.info("Local DB scan found %d symbols with 24h activity: %s",
                        len(symbols), sorted(symbols))
        return symbols

    def _scan_all_local_db_dirs(self) -> set[str]:
        """Return every symbol that has a DB directory in db_root."""
        symbols: set[str] = set()
        if not self._db_root or not os.path.isdir(self._db_root):
            return symbols

        for name in os.listdir(self._db_root):
            sym_dir = os.path.join(self._db_root, name)
            if os.path.isdir(sym_dir):
                symbols.add(name)

        if symbols:
            logger.info("Local DB dirs: %d symbols total", len(symbols))
        return symbols
