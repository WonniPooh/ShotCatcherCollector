"""
EventDispatcher — routes Binance WS events to internal components and UI.

Decouples event routing from the main orchestrator:
  - ORDER_TRADE_UPDATE → SymbolTracker + OrderEventPersister + UI broadcast
  - TRADE fills → LivePositionTracker → position_closed broadcast
  - New-symbol detection → SymbolWorkerManager trigger + UI notification
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from order_event_persister import OrderEventPersister
    from symbol_tracker import SymbolTracker
    from symbol_workers import SymbolWorkerManager
    from ws_server import CollectorWSServer

from data_manager.order_data_manager import ws_order_event_to_row
from data_manager.user_trades_manager import ws_event_to_trade_row
from data_manager.position_manager.position_tracker import LivePositionTracker

logger = logging.getLogger("collector.event_dispatcher")

# Maps Binance execution type to UI event name.
_EXEC_TO_UI_EVENT = {
    "NEW":       "order_placed",
    "CANCELED":  "order_canceled",
    "EXPIRED":   "order_canceled",
    "AMENDMENT": "order_modified",
}


class EventDispatcher:
    """Routes WS order/account events to tracker, persister, workers, and UI."""

    def __init__(
        self,
        tracker: SymbolTracker,
        persister: OrderEventPersister,
        ws_server: CollectorWSServer,
        worker_manager: SymbolWorkerManager,
    ) -> None:
        self._tracker = tracker
        self._persister = persister
        self._ws_server = ws_server
        self._workers = worker_manager
        self._position_tracker = LivePositionTracker()

    @property
    def position_tracker(self) -> LivePositionTracker:
        """Expose for startup warmup."""
        return self._position_tracker

    def on_order_event(self, event: dict) -> None:
        """Dispatch ORDER_TRADE_UPDATE to tracker, persister, and UI browsers."""
        self._tracker.on_order_event(event)
        self._persister.on_order_event(event)

        exec_type = event.get("x", "")
        if exec_type == "TRADE":
            order_status = event.get("X", "")
            if order_status == "FILLED":
                ui_event = "order_filled"
            elif order_status == "PARTIALLY_FILLED":
                ui_event = "order_partially_filled"
            else:
                return

            # Feed fill to position tracker — may produce closed position(s)
            trade_row = ws_event_to_trade_row(event)
            closed_positions = self._position_tracker.on_trade(trade_row)
            for pos in closed_positions:
                pos["id"] = hash((
                    pos["symbol"],
                    pos["entry_time_ms"],
                    pos["exit_time_ms"],
                )) & 0x7FFFFFFF
                asyncio.create_task(
                    self._ws_server.broadcast({"type": "position_closed", **pos}),
                    name=f"position-closed-{pos['symbol']}",
                )
                logger.info("Position closed: %s %s pnl=%.4f (%.2f%%)",
                            pos["symbol"], pos["side"],
                            pos["realized_pnl"], pos["pnl_pct"])
        else:
            ui_event = _EXEC_TO_UI_EVENT.get(exec_type)
            if not ui_event:
                return

        payload = {"type": "order_event", "event": ui_event}
        payload.update(ws_order_event_to_row(event))
        asyncio.create_task(
            self._ws_server.broadcast(payload),
            name=f"order-event-{event.get('s')}-{event.get('i')}",
        )

    def on_symbol_added(self, symbol: str) -> None:
        """Callback for SymbolTracker — triggers data loading for a new symbol."""
        self._workers.trigger_symbol(symbol, reason="auto")
        asyncio.create_task(
            self._ws_server.broadcast({
                "type": "auto_loaded",
                "symbol": symbol,
                "reason": "open_position",
            })
        )
