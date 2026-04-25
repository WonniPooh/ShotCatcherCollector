"""
Collector — entry point.

Thin orchestrator that wires components and manages lifecycle:
  1. BinanceFuturesClient (REST, authenticated)
  2. SymbolWorkerManager (per-symbol trades + account sync tasks)
  3. EventDispatcher (routes WS events to tracker/persister/UI)
  4. SymbolTracker (discovers and maintains watched symbol set)
  5. UserDataWS (real-time position/order events)
  6. CollectorWSServer (chart-ui-server connects here)
"""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import signal
import sys
from pathlib import Path

# Add project root to sys.path for BinanceDataManagers package imports
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config import load_config
from binance_futures_client import BinanceFuturesClient
from user_data_ws import UserDataWS
from symbol_tracker import SymbolTracker
from symbol_workers import SymbolWorkerManager
from event_dispatcher import EventDispatcher
from ws_server import CollectorWSServer
from BinanceDataManagers.trades_manager.trades_loader import TradesLoader
from BinanceDataManagers.binance_rate_limiter import bnx_limiter
from order_event_persister import OrderEventPersister
from dashboard import Dashboard


def _setup_logging(log_dir: str = "logs") -> None:
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)

    file_handler = logging.handlers.RotatingFileHandler(
        f"{log_dir}/collector.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(console)
    root.addHandler(file_handler)


logger = logging.getLogger("collector")


def _warm_position_tracker(tracker: "LivePositionTracker", db_root: str) -> None:
    """Replay existing user_trades.db to establish running position state."""
    import sqlite3
    root = Path(db_root)
    if not root.is_dir():
        return
    warmed = 0
    for sym_dir in sorted(root.iterdir()):
        if not sym_dir.is_dir():
            continue
        ut_path = sym_dir / "user_trades.db"
        if not ut_path.exists() or ut_path.stat().st_size == 0:
            continue
        symbol = sym_dir.name
        try:
            conn = sqlite3.connect(str(ut_path))
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM user_trade WHERE symbol = ? ORDER BY trade_time_ms",
                (symbol,),
            )
            trades = [dict(row) for row in cur.fetchall()]
            conn.close()
            if trades:
                tracker.warm(symbol, trades)
                warmed += 1
        except Exception as exc:
            logger.warning("Failed to warm position tracker for %s: %s", symbol, exc)
    logger.info("Position tracker warmed from %d symbols", warmed)


async def main() -> None:
    _setup_logging()
    cfg = load_config()

    if "--no-trades" in sys.argv:
        cfg.trades_enabled = False

    if not cfg.api_key:
        logger.error("No API key configured — cannot start collector")
        return

    logger.info("Starting collector (db_root=%s, ws_port=%d)", cfg.db_root, cfg.ws_port)

    # ── Create components ─────────────────────────────────────────────

    status_queue: asyncio.Queue = asyncio.Queue()

    # Workers need ws_server reference — created below, wired via lambdas
    workers: SymbolWorkerManager | None = None

    trades_loader = TradesLoader(
        db_root=cfg.db_root,
        rate_limiter=bnx_limiter,
        status_queue=status_queue,
        on_reconnect_gap=lambda sym, pre, post: workers.on_reconnect_gap(sym, pre, post),
    )
    trades_loader.start()

    rest_client = BinanceFuturesClient(
        api_key=cfg.api_key,
        api_secret=cfg.api_secret,
        rate_limiter=bnx_limiter,
    )

    persister = OrderEventPersister(db_root=cfg.db_root, rest_client=rest_client)

    # Workers need ws_server reference — created below, wired via lambdas
    workers: SymbolWorkerManager | None = None

    ws_server = CollectorWSServer(
        host=cfg.ws_host,
        port=cfg.ws_port,
        on_load_request=lambda sym: workers.on_ui_load_request(sym),
        on_cancel_request=lambda sym: workers.cancel_symbol(sym),
        get_status=lambda sym: workers.get_status(sym),
        rest_client=rest_client,
        on_fill_gap=lambda sym, f, t: workers.on_ui_fill_gap(sym, f, t),
    )
    await ws_server.start()

    workers = SymbolWorkerManager(
        cfg=cfg,
        ws_server=ws_server,
        rest_client=rest_client,
        trades_loader=trades_loader,
        persister=persister,
        status_queue=status_queue,
        full_order_resync="--full-order-resync" in sys.argv,
    )
    workers.start()

    # EventDispatcher needs tracker — created below, wired after
    dispatcher: EventDispatcher | None = None

    tracker = SymbolTracker(
        rest_client=rest_client,
        on_symbol_added=lambda sym: dispatcher.on_symbol_added(sym),
        account_sync_interval_min=cfg.account_sync_interval_min,
        symbol_inactive_prune_days=cfg.symbol_inactive_prune_days,
        db_root=cfg.db_root,
    )

    dispatcher = EventDispatcher(
        tracker=tracker,
        persister=persister,
        ws_server=ws_server,
        worker_manager=workers,
    )

    # Warm up the position tracker from existing user_trades.db files
    # so it knows the running position state before live events arrive.
    _warm_position_tracker(dispatcher.position_tracker, cfg.db_root)

    user_data_ws = UserDataWS(
        rest_client,
        on_order_event=dispatcher.on_order_event,
        on_account_update=tracker.on_account_update,
    )

    # ── Start ─────────────────────────────────────────────────────────

    user_data_ws.start()
    await tracker.start()

    # Drain buffered WS events received during startup
    buffered = user_data_ws.drain_buffer()
    if buffered:
        # Route through dispatcher so tracker detects new symbols from buffered events
        for event in buffered:
            dispatcher.on_order_event(event)
        logger.info("Dispatched %d buffered WS events from startup", len(buffered))

    logger.info("Collector running — watching %d symbols", len(tracker.watched_symbols))

    dashboard: Dashboard | None = None
    if "--no-dashboard" not in sys.argv:
        dashboard = Dashboard(
            user_data_ws=user_data_ws,
            symbol_tracker=tracker,
            trades_loader=trades_loader,
            persister=persister,
            rate_limiter=bnx_limiter,
        )
        dashboard.start()

    # ── Wait for shutdown signal ──────────────────────────────────────

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_signal() -> None:
        if stop_event.is_set():
            logger.warning("Force shutdown — second signal received")
            os._exit(1)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _on_signal)

    try:
        await stop_event.wait()
    finally:
        if dashboard:
            await dashboard.stop()
        logger.info("Shutting down collector...")
        await workers.shutdown()
        await trades_loader.stop()
        await tracker.stop()
        await user_data_ws.stop()
        persister.close()
        await ws_server.stop()
        await rest_client.close()
        logger.info("Collector stopped")


if __name__ == "__main__":
    asyncio.run(main())
