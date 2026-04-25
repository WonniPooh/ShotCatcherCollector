"""
SymbolWorkerManager — owns per-symbol async task lifecycle.

Manages trades loading, account sync, and UI load prioritization.
Consumes TradesLoader status events and broadcasts progress to UI clients.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import CollectorConfig
    from order_event_persister import OrderEventPersister
    from ws_server import CollectorWSServer
    from data_manager.trades_manager.trades_loader import TradesLoader

from account_data_loader import download_account_data_for_symbol

logger = logging.getLogger("collector.symbol_workers")


class SymbolWorkerManager:
    """Manages per-symbol trades + account-sync tasks and status broadcasting."""

    def __init__(
        self,
        cfg: CollectorConfig,
        ws_server: CollectorWSServer,
        rest_client,
        trades_loader: TradesLoader,
        persister: OrderEventPersister,
        status_queue: asyncio.Queue,
        full_order_resync: bool = False,
    ) -> None:
        self._cfg = cfg
        self._ws_server = ws_server
        self._rest_client = rest_client
        self._trades_loader = trades_loader
        self._persister = persister
        self._status_queue = status_queue

        self._trades_tasks: dict[str, asyncio.Task] = {}
        self._account_tasks: dict[str, asyncio.Task] = {}
        self._ui_load_symbol: str | None = None
        self._full_order_resync = full_order_resync

        self._status_task: asyncio.Task | None = None

    def start(self) -> None:
        """Start the status-queue consumer loop."""
        self._status_task = asyncio.create_task(
            self._consume_status_queue(), name="status-queue-consumer"
        )

    # ── Public API ────────────────────────────────────────────────────

    def get_status(self, symbol: str | None) -> dict:
        if symbol:
            task = self._trades_tasks.get(symbol)
            state = "idle"
            if task and not task.done():
                state = "loading"
            elif task and task.done():
                state = "done"
            return {"symbol": symbol, "state": state, "pct": 0, "phase": "trades"}
        return {"symbols": sorted(self._trades_tasks.keys())}

    def on_ui_load_request(self, symbol: str) -> None:
        """Handle an explicit load request from the chart UI.

        Cancels any other symbol that was previously requested by the UI
        (but not system-auto-loaded symbols) so the focused symbol always
        gets all the bandwidth.
        """
        logger.info("[%s] UI load request received (prev_ui_symbol=%s)",
                     symbol, self._ui_load_symbol or "none")

        if self._ui_load_symbol == symbol:
            logger.info("[%s] Already the active UI symbol — sending current state", symbol)
            # Don't re-trigger loading, but tell the (re-)connected browser
            # whether the trades phase has already completed so it doesn't
            # stay stuck on a loading bar.
            trades_task = self._trades_tasks.get(symbol)
            if trades_task and trades_task.done():
                asyncio.create_task(self._ws_server.broadcast({
                    "type": "done", "symbol": symbol, "phase": "trades",
                }))
            account_task = self._account_tasks.get(symbol)
            if account_task and account_task.done():
                asyncio.create_task(self._ws_server.broadcast({
                    "type": "done", "symbol": symbol, "phase": "account",
                }))
            return

        if self._ui_load_symbol and self._ui_load_symbol != symbol:
            logger.info("UI switched %s → %s, cancelling previous load",
                        self._ui_load_symbol, symbol)
            self._trades_loader.cancel(self._ui_load_symbol)

        self._ui_load_symbol = symbol
        self.trigger_symbol(symbol, priority=True)

    def trigger_symbol(self, symbol: str, reason: str = "request",
                       priority: bool = False) -> None:
        """Start trades + account workers for a symbol if not already running."""
        logger.info("[%s] trigger_symbol  reason=%s  priority=%s",
                    symbol, reason, priority)

        existing = self._trades_tasks.get(symbol)
        if existing and not existing.done() and priority:
            logger.info("[%s] Cancelling non-priority trades task for priority UI load", symbol)
            existing.cancel()
            self._trades_loader.cancel(symbol)

        if existing is None or existing.done() or priority:
            self._trades_tasks[symbol] = asyncio.create_task(
                self._run_trades(symbol, priority=priority),
                name=f"trades-{symbol}",
            )

        if symbol not in self._account_tasks or self._account_tasks[symbol].done():
            self._account_tasks[symbol] = asyncio.create_task(
                self._run_account_sync(symbol),
                name=f"account-{symbol}",
            )

    def cancel_symbol(self, symbol: str) -> None:
        logger.info("[%s] cancel_symbol called", symbol)
        for tasks in (self._trades_tasks, self._account_tasks):
            task = tasks.get(symbol)
            if task and not task.done():
                task.cancel()
        self._trades_loader.cancel(symbol)

    async def shutdown(self) -> None:
        """Cancel all per-symbol tasks and the status consumer."""
        pending: list[asyncio.Task] = []
        for tasks in (self._trades_tasks, self._account_tasks):
            for task in tasks.values():
                if not task.done():
                    task.cancel()
                    pending.append(task)

        for task in asyncio.all_tasks():
            if task is not asyncio.current_task() and not task.done():
                name = task.get_name()
                if name.startswith("trades-") or name.startswith("order-event-"):
                    task.cancel()
                    pending.append(task)

        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        if self._status_task:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

    # ── Workers ───────────────────────────────────────────────────────

    def on_reconnect_gap(self, symbol: str, pre_ts: int, post_ts: int) -> None:
        """Called by TradesWSStream when a WS reconnect gap is detected.

        Schedules a background task to fill the gap via REST.  Runs on the
        event-loop thread (callback comes from the WS stream _on_trade path).
        """
        gap_s = (post_ts - pre_ts) / 1000
        logger.info("[%s] reconnect gap callback: %d → %d (%.0fs) — scheduling fill",
                    symbol, pre_ts, post_ts, gap_s)
        asyncio.create_task(
            self._fill_gap(symbol, pre_ts, post_ts, priority=False),
            name=f"reconnect-fill-{symbol}",
        )

    def on_ui_fill_gap(self, symbol: str, pre_ts: int, post_ts: int) -> None:
        """Called by the chart-ui-server to fill a specific gap (user-facing, priority)."""
        gap_s = (post_ts - pre_ts) / 1000
        logger.info("[%s] UI fill_gap request: %d → %d (%.0fs) — scheduling priority fill",
                    symbol, pre_ts, post_ts, gap_s)
        asyncio.create_task(
            self._fill_gap(symbol, pre_ts, post_ts, priority=True),
            name=f"ui-fill-gap-{symbol}",
        )

    async def _fill_gap(self, symbol: str, pre_ts: int, post_ts: int,
                        *, priority: bool = False) -> None:
        """Fill a trade gap by queuing a targeted REST load (no archive)."""
        try:
            inserted = await self._trades_loader.load_recent(
                symbol, days=1, priority=priority,
                end_ms=post_ts, pre_ws_max_ts=pre_ts,
                skip_archive=True,
            )
            logger.info("[%s] gap fill done  inserted=%d  priority=%s", symbol, inserted, priority)
            await self._ws_server.broadcast({
                "type": "fill_gap_done",
                "symbol": symbol,
                "from_ms": pre_ts,
                "to_ms": post_ts,
                "inserted": inserted,
            })
        except asyncio.CancelledError:
            logger.info("[%s] gap fill cancelled", symbol)
        except Exception as exc:
            logger.error("[%s] gap fill failed: %s", symbol, exc, exc_info=True)

    async def _run_trades(self, symbol: str, *, priority: bool = False) -> None:
        try:
            pre_ws_max_ts = self._trades_loader.get_last_trade_ts(symbol)

            far_future_ms = int(time.time() * 1000) + 365 * 24 * 60 * 60 * 1000
            logger.info("[%s] _run_trades: starting live WS stream (pre_ws_max_ts=%s)",
                        symbol, pre_ws_max_ts)
            await self._trades_loader.start_live(symbol, until_ms=far_future_ms)

            first_ts = await self._trades_loader.wait_for_first_trade(symbol, timeout=30.0)
            if first_ts:
                logger.info("[%s] _run_trades: first WS trade at %d — REST will fill up to this point",
                            symbol, first_ts)
            else:
                logger.warning("[%s] _run_trades: no WS trade within 30s — falling back to now+5s",
                               symbol)

            # Record the gap between old data and first WS trade BEFORE
            # flushing — if the REST fill is cancelled later the gap row
            # survives so the next load_recent sees it.
            if pre_ws_max_ts is not None and first_ts is not None:
                self._trades_loader.record_bridge_gap(symbol, pre_ws_max_ts, first_ts)

            flushed = await self._trades_loader.flush_live(symbol)
            if flushed:
                logger.info("[%s] _run_trades: force-flushed %d buffered trades", symbol, flushed)

            logger.info("[%s] _run_trades: queueing load_recent(days=7, priority=%s)",
                        symbol, priority)
            inserted = await self._trades_loader.load_recent(
                symbol, days=7, priority=priority, end_ms=first_ts,
                pre_ws_max_ts=pre_ws_max_ts,
            )
            logger.info("[%s] _run_trades: load_recent complete  inserted=%d", symbol, inserted)
            await self._ws_server.broadcast({
                "type": "done", "symbol": symbol, "phase": "trades",
                "trades": inserted,
            })
        except asyncio.CancelledError:
            logger.info("[%s] _run_trades: CANCELLED", symbol)
        except Exception as exc:
            logger.error("Trades worker failed for %s: %s", symbol, exc, exc_info=True)
            await self._ws_server.broadcast({
                "type": "error", "symbol": symbol, "phase": "trades", "msg": str(exc),
            })

    async def _run_account_sync(self, symbol: str, attempt: int = 1) -> None:
        max_retries = 3
        retry_delay_s = 30
        try:
            result = await download_account_data_for_symbol(
                self._cfg.db_root, symbol, self._rest_client,
                full_resync=self._full_order_resync,
            )
            self._persister.flush_cached_amendments(symbol)
            await self._ws_server.broadcast({
                "type": "done",
                "symbol": symbol,
                "phase": "account",
                "orders": result["orders"],
                "trades": result["trades"],
                "amendments": result["amendments"],
            })
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Account sync failed for %s (attempt %d/%d): %s",
                         symbol, attempt, max_retries, exc)
            await self._ws_server.broadcast({
                "type": "error", "symbol": symbol, "phase": "account", "msg": str(exc),
            })
            if attempt < max_retries:
                logger.info("[%s] Retrying account sync in %ds (attempt %d/%d)",
                            symbol, retry_delay_s, attempt + 1, max_retries)
                await asyncio.sleep(retry_delay_s)
                await self._run_account_sync(symbol, attempt=attempt + 1)
        finally:
            # One-shot: only the first sync round uses full resync
            self._full_order_resync = False

    # ── Status queue consumer ─────────────────────────────────────────

    async def _consume_status_queue(self) -> None:
        """Consume TradesLoader status events and broadcast progress to UI."""
        while True:
            try:
                msg = await self._status_queue.get()
            except asyncio.CancelledError:
                return

            symbol_key = msg.get("symbol", "")
            source = msg.get("source", "")
            phase = msg.get("phase", "")
            pct = msg.get("pct", 0)

            logger.debug("[%s] status_queue: source=%s phase=%s pct=%s detail=%s",
                         symbol_key, source, phase, pct, msg.get("detail", ""))

            if phase == "done" and source in ("rest", "archive", "loader"):
                logger.info("[%s] → broadcasting trades DONE (source=%s)", symbol_key, source)
                payload: dict = {
                    "type": "progress", "symbol": symbol_key,
                    "phase": "trades", "pct": 100,
                }
            elif phase in ("loading", "download", "insert", "archive-phase"):
                payload = {
                    "type": "progress", "symbol": symbol_key,
                    "phase": "trades", "pct": pct,
                }
                if "covered_from_ms" in msg:
                    payload["covered_from_ms"] = msg["covered_from_ms"]
                if "covered_to_ms" in msg:
                    payload["covered_to_ms"] = msg["covered_to_ms"]
            else:
                continue

            asyncio.create_task(
                self._ws_server.broadcast(payload),
                name=f"status-broadcast-{symbol_key}",
            )
