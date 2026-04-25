"""
OrderEventPersister — persists UserDataWS ORDER_TRADE_UPDATE events to SQLite.

Manages per-symbol DB handles (opened on demand, cached for reuse).
Maps parsed WS events (short Binance keys) to DB row format using mappers
from BinanceDataManagers.order_BinanceDataManagers and BinanceDataManagers.user_trades_manager.

Amendment handling:
  Tracks (price, qty) per order in memory.  When an AMENDMENT event arrives:
    - Pre-sync (REST hasn't loaded amendments yet): cache the amendment.
    - Post-sync: write directly to order_amendment with tracked price_before.
  After REST sync completes for a symbol, flush_cached_amendments() loads the
  last REST-fetched price per order, deduplicates, and writes remaining WS
  amendments.

Self-healing:
  When an amendment arrives for an order not in _order_prices (e.g. NEW event
  lost during WS reconnection), the persister queues a REST repair:
    1. Fetch the order via allOrders + amendments via orderAmendment
    2. Insert missing data into DB
    3. Warm _order_prices and write the pending amendment
  A _pending_repairs set prevents duplicate fetches for the same order.

Usage:
    persister = OrderEventPersister(db_root="/data/symbols")
    user_data_ws = UserDataWS(..., on_order_event=persister.on_order_event)
    persister.persist_batch(buffered_events)
    persister.flush_cached_amendments("BTCUSDT")  # after REST sync
    persister.close()
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

_project_root = Path(__file__).resolve().parent.parent
_dm_path = str(_project_root / "BinanceDataManagers")
if _dm_path not in sys.path:
    sys.path.insert(0, _dm_path)

for _sub in ("order_BinanceDataManagers", "user_trades_manager"):
    _sp = str(_project_root / "BinanceDataManagers" / _sub)
    if _sp not in sys.path:
        sys.path.insert(0, _sp)

from order_BinanceDataManagers import OrderEventDB, ws_order_event_to_row, sync_amendments_for_order
from user_trades_manager import UserTradeDB, ws_event_to_trade_row

logger = logging.getLogger("collector.order_event_persister")

_TERMINAL_STATUSES = frozenset({"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"})

_STOP_ORDER_TYPES = frozenset({"STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET",
                                "STOP", "TAKE_PROFIT"})


def _effective_price(event: Dict[str, Any]) -> float:
    """Return stop_price for SL/TP orders, order_price otherwise."""
    order_type = event.get("o", "")
    sp = float(event.get("sp", 0))
    if sp > 0 and order_type in _STOP_ORDER_TYPES:
        return sp
    return float(event.get("p", 0))


class OrderEventPersister:
    """Persists WS order events to per-symbol SQLite databases."""

    def __init__(self, db_root: str, rest_client: Optional[Any] = None) -> None:
        self._db_root = db_root
        self._rest_client = rest_client
        self._order_dbs: Dict[str, OrderEventDB] = {}
        self._trade_dbs: Dict[str, UserTradeDB] = {}
        # In-memory price/qty tracker: order_id → (price, qty)
        self._order_prices: Dict[int, Tuple[float, float]] = {}
        # Symbols where REST sync has completed (safe to write amendments)
        self._synced_symbols: set[str] = set()
        # Cached amendments received before REST sync completes
        self._cached_amendments: List[Dict[str, Any]] = []
        # Self-healing: orders currently being repaired via REST
        self._pending_repairs: set[int] = set()
        # Amendments queued while a repair is in flight: order_id → [events]
        self._repair_queue: Dict[int, List[Dict[str, Any]]] = {}

    def on_order_event(self, event: Dict[str, Any]) -> None:
        """Callback for UserDataWS — persists a single parsed ORDER_TRADE_UPDATE."""
        symbol = event.get("s", "")
        if not symbol:
            logger.warning("order_event_persister: event missing symbol, skipping")
            return

        order_id = int(event.get("i", 0))
        exec_type = event.get("x", "")

        try:
            order_db = self._get_order_db(symbol)
            order_row = ws_order_event_to_row(event)
            order_db.insert_one(order_row)
        except Exception:
            logger.error("order_event_persister: failed to write order_event "
                         "%s %s for %s order=%d",
                         exec_type, event.get("X"), symbol, order_id,
                         exc_info=True)
            return

        # If this is a fill, also insert into user_trades
        if event.get("x") == "TRADE" and event.get("t", 0) > 0:
            try:
                trade_db = self._get_trade_db(symbol)
                trade_row = ws_event_to_trade_row(event)
                trade_db.insert_one(trade_row)
            except Exception:
                logger.error("order_event_persister: failed to write user_trade "
                             "for %s order=%d trade=%d",
                             symbol, order_id, event.get("t", 0),
                             exc_info=True)

        price = _effective_price(event)
        qty = float(event.get("q", 0))

        if exec_type == "NEW":
            self._order_prices[order_id] = (price, qty)
        elif exec_type == "AMENDMENT":
            self._write_amendment(symbol, event)
            # Only update price tracker if the amendment was actually written
            # (not cached pre-sync, not queued for repair)
            if symbol in self._synced_symbols and order_id not in self._pending_repairs:
                if order_id in self._order_prices:
                    self._order_prices[order_id] = (price, qty)
        elif event.get("X", "") in _TERMINAL_STATUSES:
            self._order_prices.pop(order_id, None)

        logger.debug(
            "persisted %s %s for %s order=%d",
            exec_type, event.get("X"), symbol, order_id,
        )

    def persist_batch(self, events: List[Dict[str, Any]]) -> None:
        """Persist a batch of buffered events (e.g. from drain_buffer()).

        Continues past individual failures so one bad event doesn't kill the batch.
        """
        failed = 0
        for event in events:
            try:
                self.on_order_event(event)
            except Exception:
                failed += 1
                logger.error("order_event_persister: batch event failed for %s",
                             event.get("s", "?"), exc_info=True)
        if events:
            logger.info("order_event_persister: persisted %d buffered events (%d failed)",
                        len(events) - failed, failed)

    def flush_cached_amendments(self, symbol: str) -> int:
        """Called after REST sync completes for a symbol.

        Loads last REST-fetched amendment prices from DB, processes cached WS
        amendments for this symbol, deduplicates, and writes new ones.
        Returns number of amendments written.
        """
        order_db = self._get_order_db(symbol)

        # Warm up price tracker from REST-loaded amendments
        latest = order_db.get_latest_amendment_prices(symbol)
        for order_id, (price, qty) in latest.items():
            # Only update if we don't already have a tracked price
            # (WS events processed before flush take precedence)
            if order_id not in self._order_prices:
                self._order_prices[order_id] = (price, qty)

        # Also warm from order_event table for orders without amendments
        # (e.g. NEW event was in REST sync but no amendments yet)
        # Use stop_price for TP/SL orders, order_price otherwise
        rows = order_db.conn.execute(
            "SELECT DISTINCT order_id, order_price, order_qty, stop_price, order_type "
            "FROM order_event "
            "WHERE symbol = ? AND order_status NOT IN "
            "('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED') "
            "ORDER BY transaction_time_ms DESC",
            (symbol,),
        ).fetchall()
        for row in rows:
            oid = row[0]
            if oid not in self._order_prices:
                order_price, order_qty, stop_price, order_type = row[1], row[2], row[3], row[4]
                effective = stop_price if stop_price > 0 and order_type in _STOP_ORDER_TYPES else order_price
                self._order_prices[oid] = (effective, order_qty)

        # Process cached amendments for this symbol
        remaining = []
        written = 0
        for cached_evt in self._cached_amendments:
            if cached_evt.get("s") != symbol:
                remaining.append(cached_evt)
                continue
            order_id = int(cached_evt.get("i", 0))
            price = _effective_price(cached_evt)
            qty = float(cached_evt.get("q", 0))
            old = self._order_prices.get(order_id)
            if old is not None:
                old_price, old_qty = old
                order_db.insert_amendment_rows([{
                    "amendment_id": None,
                    "order_id": order_id,
                    "symbol": symbol,
                    "client_order_id": cached_evt.get("c", ""),
                    "time_ms": int(cached_evt.get("T", 0)),
                    "price_before": old_price,
                    "price_after": price,
                    "qty_before": old_qty,
                    "qty_after": qty,
                    "amendment_count": 0,
                }])
                written += 1
            else:
                logger.warning(
                    "flush_cached_amendments: no tracked price for %s order=%d, skipping",
                    symbol, order_id,
                )
            self._order_prices[order_id] = (price, qty)

        self._cached_amendments = remaining
        self._synced_symbols.add(symbol)

        if written:
            logger.info("[%s] flush_cached_amendments: wrote %d WS amendments", symbol, written)
        return written

    def close(self) -> None:
        """Close all cached DB handles."""
        for db in self._order_dbs.values():
            db.close()
        for db in self._trade_dbs.values():
            db.close()
        self._order_dbs.clear()
        self._trade_dbs.clear()
        logger.info("order_event_persister: closed all DB handles")

    # ── Internal ──────────────────────────────────────────────────────────

    def _write_amendment(self, symbol: str, event: Dict[str, Any]) -> None:
        """Write an amendment row to DB (or cache if not synced yet).

        If the order is not in _order_prices (e.g. NEW event lost during WS
        reconnection), queue the amendment and trigger a REST repair to fetch
        the order's data.
        """
        order_id = int(event.get("i", 0))
        price = _effective_price(event)
        qty = float(event.get("q", 0))

        if symbol not in self._synced_symbols:
            self._cached_amendments.append(event)
            return

        old = self._order_prices.get(order_id)
        if old is None:
            # Self-heal: queue this amendment and fetch order data from REST
            self._repair_queue.setdefault(order_id, []).append(event)
            if order_id not in self._pending_repairs and self._rest_client is not None:
                self._pending_repairs.add(order_id)
                logger.warning(
                    "amendment for untracked order %s order=%d — queuing REST repair",
                    symbol, order_id,
                )
                asyncio.create_task(
                    self._repair_order(symbol, order_id),
                    name=f"repair-order-{symbol}-{order_id}",
                )
            elif self._rest_client is None:
                logger.warning(
                    "amendment for untracked order %s order=%d — no REST client, skipping",
                    symbol, order_id,
                )
            return

        old_price, old_qty = old
        order_db = self._get_order_db(symbol)
        order_db.insert_amendment_rows([{
            "amendment_id": None,
            "order_id": order_id,
            "symbol": symbol,
            "client_order_id": event.get("c", ""),
            "time_ms": int(event.get("T", 0)),
            "price_before": old_price,
            "price_after": price,
            "qty_before": old_qty,
            "qty_after": qty,
            "amendment_count": 0,
        }])

    def _get_order_db(self, symbol: str) -> OrderEventDB:
        if symbol not in self._order_dbs:
            sym_dir = Path(self._db_root) / symbol
            sym_dir.mkdir(parents=True, exist_ok=True)
            self._order_dbs[symbol] = OrderEventDB(str(sym_dir / "order_events.db"))
        return self._order_dbs[symbol]

    def _get_trade_db(self, symbol: str) -> UserTradeDB:
        if symbol not in self._trade_dbs:
            sym_dir = Path(self._db_root) / symbol
            sym_dir.mkdir(parents=True, exist_ok=True)
            self._trade_dbs[symbol] = UserTradeDB(str(sym_dir / "user_trades.db"))
        return self._trade_dbs[symbol]

    async def _repair_order(self, symbol: str, order_id: int) -> None:
        """Fetch order + amendments from REST to fill a gap left by a missed NEW event.

        After fetching:
          1. Insert order events into DB (ON CONFLICT → no-op for existing)
          2. Insert REST amendments (authoritative, with real amendment_id)
          3. Warm _order_prices from the fetched data
          4. Process any WS amendments that were queued during the repair
        """
        try:
            logger.info("[%s] repair_order: fetching order %d from REST", symbol, order_id)
            order_db = self._get_order_db(symbol)

            # Fetch order events — allOrders with orderId filter
            orders = await self._rest_client.get_orders(
                symbol=symbol, order_id=order_id, limit=10,
            )
            target_orders = [o for o in orders if int(o["orderId"]) == order_id]
            if target_orders:
                from order_BinanceDataManagers.order_data_loader import _live_order_to_row
                rows = [_live_order_to_row(o) for o in target_orders]
                order_db.insert_rows(rows)
                logger.info("[%s] repair_order: inserted %d order events for order %d",
                            symbol, len(rows), order_id)

                # Warm price tracker from the fetched order
                for o in target_orders:
                    order_type = o.get("origType", o.get("type", "LIMIT"))
                    stop_price = float(o.get("stopPrice", 0))
                    order_price = float(o.get("price", 0))
                    order_qty = float(o.get("origQty", 0))
                    effective = stop_price if stop_price > 0 and order_type in _STOP_ORDER_TYPES else order_price
                    if order_id not in self._order_prices:
                        self._order_prices[order_id] = (effective, order_qty)

            # Fetch REST amendments (authoritative — have real amendment_id)
            amend_count = await sync_amendments_for_order(
                self._rest_client, symbol, order_id, order_db,
            )
            if amend_count:
                logger.info("[%s] repair_order: inserted %d REST amendments for order %d",
                            symbol, amend_count, order_id)
                # Update price tracker from the latest amendment
                latest = order_db.get_latest_amendment_prices(symbol)
                if order_id in latest:
                    self._order_prices[order_id] = latest[order_id]

            # Process queued WS amendments for this order
            queued = self._repair_queue.pop(order_id, [])
            written = 0
            for evt in queued:
                old = self._order_prices.get(order_id)
                if old is None:
                    logger.warning("[%s] repair_order: still no price for order %d after REST fetch, "
                                   "dropping %d queued amendments", symbol, order_id, len(queued) - written)
                    break
                old_price, old_qty = old
                price = _effective_price(evt)
                qty = float(evt.get("q", 0))
                order_db.insert_amendment_rows([{
                    "amendment_id": None,
                    "order_id": order_id,
                    "symbol": symbol,
                    "client_order_id": evt.get("c", ""),
                    "time_ms": int(evt.get("T", 0)),
                    "price_before": old_price,
                    "price_after": price,
                    "qty_before": old_qty,
                    "qty_after": qty,
                    "amendment_count": 0,
                }])
                self._order_prices[order_id] = (price, qty)
                written += 1

            if written:
                logger.info("[%s] repair_order: wrote %d queued WS amendments for order %d",
                            symbol, written, order_id)

        except Exception:
            logger.error("[%s] repair_order failed for order %d", symbol, order_id, exc_info=True)
            # Drop queued amendments — they'd be picked up on next REST sync anyway
            self._repair_queue.pop(order_id, None)
        finally:
            self._pending_repairs.discard(order_id)

    # ── Test hooks ────────────────────────────────────────────────────────

    def get_order_db_for_test(self, symbol: str) -> OrderEventDB:
        """Test hook: return the cached OrderEventDB for a symbol."""
        return self._get_order_db(symbol)

    def get_trade_db_for_test(self, symbol: str) -> UserTradeDB:
        """Test hook: return the cached UserTradeDB for a symbol."""
        return self._get_trade_db(symbol)
