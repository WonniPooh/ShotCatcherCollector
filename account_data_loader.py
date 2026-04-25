"""
Account data loader — orchestrates order + trade + amendment sync for a symbol.

Delegates all sync logic and row mapping to BinanceDataManagers modules.
This file only owns: DB open/close lifecycle and start-time calculus
(89-day cap, 2h lookback on restart).
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict

_project_root = Path(__file__).resolve().parent.parent
# Add BinanceDataManagers and each sub-package dir so that bare imports inside those
# modules (e.g. `from order_events_db_manager import ...`) also resolve.
for _sub in (
    "",
    "order_data_manager",
    "user_trades_manager",
):
    _sp = str(_project_root / "BinanceDataManagers" / _sub)
    if _sp not in sys.path:
        sys.path.insert(0, _sp)

from order_events_db_manager import OrderEventDB
from BinanceDataManagers.order_data_manager.order_data_loader import sync_orders, sync_amendments
from user_trades_manager import UserTradeDB, sync_trades

logger = logging.getLogger("collector.account_data_loader")

# Re-fetch 2h before last sync to pick up late terminal events
_LOOKBACK_MS    = 2 * 60 * 60 * 1000
# Binance rejects exactly 90 days back (-4166); use 89 days
_MAX_HISTORY_MS = 89 * 24 * 60 * 60 * 1000


async def download_account_data_for_symbol(
    db_root: str,
    symbol: str,
    client: Any,  # BinanceFuturesClient
    full_resync: bool = False,
) -> Dict[str, int]:
    """
    Sync order events, user trades, and order amendments for `symbol`.

    If full_resync=True, ignores last_sync_ts and syncs from 89 days ago.
    Returns counts: {"orders": N, "trades": N, "amendments": N}
    """
    sym_dir = Path(db_root) / symbol
    sym_dir.mkdir(parents=True, exist_ok=True)

    order_db     = OrderEventDB(str(sym_dir / "order_events.db"))
    trade_db     = UserTradeDB(str(sym_dir / "user_trades.db"))

    try:
        now_ms = int(time.time() * 1000)
        oldest_allowed = now_ms - _MAX_HISTORY_MS

        if full_resync:
            start_time = oldest_allowed
            logger.info("[%s] full-order-resync: ignoring last_sync_ts, starting from 89d ago", symbol)
        else:
            # Prefer saved sync timestamp; fall back to latest row in DB
            saved = order_db.get_meta("last_sync_ts")
            if saved:
                start_time = max(0, int(saved) - _LOOKBACK_MS)
            else:
                last_order = order_db.get_latest_transaction_time() or 0
                last_trade = trade_db.get_latest_trade_time() or 0
                non_zero   = [ts for ts in (last_order, last_trade) if ts > 0]
                start_time = max(0, max(non_zero) - _LOOKBACK_MS) if non_zero else 0

        if start_time < oldest_allowed:
            start_time = oldest_allowed

        logger.info(
            "[%s] account sync from %d (%.1fh ago)",
            symbol, start_time, (now_ms - start_time) / 3_600_000,
        )

        orders_count = 0
        trades_count = 0
        amendments_count = 0

        try:
            orders_count = await sync_orders(client, symbol, start_time, now_ms, order_db)
            order_db.set_meta("last_sync_ts", str(now_ms))
        except Exception:
            logger.error("[%s] sync_orders failed", symbol, exc_info=True)

        try:
            trades_count = await sync_trades(client, symbol, start_time, now_ms, trade_db)
            trade_db.set_meta("last_sync_ts", str(now_ms))
        except Exception:
            logger.error("[%s] sync_trades failed", symbol, exc_info=True)

        try:
            amendments_count = await sync_amendments(client, symbol, start_time, order_db)
        except Exception:
            logger.error("[%s] sync_amendments failed", symbol, exc_info=True)

        logger.info(
            "[%s] account sync done — %d orders, %d trades, %d amendments",
            symbol, orders_count, trades_count, amendments_count,
        )
        return {"orders": orders_count, "trades": trades_count, "amendments": amendments_count}

    finally:
        order_db.close()
        trade_db.close()
