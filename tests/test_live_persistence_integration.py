# test_live_persistence_integration.py
"""
Integration test: live order event persistence on Binance mainnet.

Verifies the full pipeline: order placement → WS event → OrderEventPersister → DB.
Covers LIMIT orders, STOP (stop-limit) orders, fills, cancellations, and EXPIRED
reduce-only orders.

⚠️  MAINNET — uses real funds.  Test orders use tiny notional (~6 USDT) and
    distinct clientOrderId prefix (SC_INT_TEST_) to avoid touching any existing
    open orders.  All test orders are cleaned up in the finally block.

Requires: config/secrets.json with valid trading API credentials.

Run manually:
    python -m collector.tests.test_live_persistence_integration

Test plan:
  1. Connect UserDataWS, wait for live mode
  2. Place LIMIT BUY far below market → verify NEW event persisted
  3. Cancel the LIMIT BUY → verify CANCELED event persisted
  4. Place MARKET BUY (tiny qty) → verify TRADE event + user_trade persisted
  5. Place algo STOP SELL (conditional SL) → verify REST response (strategyId)
     Note: algo orders use /fapi/v1/order/algo/stop and generate STRATEGY_UPDATE
     WS events (not ORDER_TRADE_UPDATE), so we verify REST round-trip only.
  6. Cancel the algo STOP → verify via REST
  7. Place LIMIT SELL reduce-only at far-above price → verify NEW
  8. Close position with MARKET SELL → reduce-only LIMIT gets EXPIRED
  9. Verify EXPIRED event persisted
  10. Place + cancel another algo STOP round-trip
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "collector"))
sys.path.insert(0, str(_project_root / "data_manager"))
sys.path.insert(0, str(_project_root / "data_manager" / "order_data_manager"))
sys.path.insert(0, str(_project_root / "data_manager" / "user_trades_manager"))

from collector.binance_futures_client import BinanceFuturesClient, BinanceAPIError
from collector.user_data_ws import UserDataWS
from collector.order_event_persister import OrderEventPersister

# ── Config ──────────────────────────────────────────────────────────────

SECRETS_PATH = _project_root / "config" / "secrets.json"
TEST_DB_DIR = str(_project_root / "logs" / "test_live_persistence")
TEST_SYMBOL = "ADAUSDT"
# Prefix for all test orders — used for cleanup identification
CLIENT_ID_PREFIX = "SC_INT_TEST_"

# ADAUSDT: minQty=1, stepSize=1, minNotional=5 USDT
# At ~0.7 USDT/ADA, 10 ADA ≈ 7 USDT (above minNotional)
TEST_QTY = "40"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("test_live_persistence")


def load_credentials() -> dict:
    if not SECRETS_PATH.exists():
        raise FileNotFoundError(f"secrets.json not found at {SECRETS_PATH}")
    with open(SECRETS_PATH) as f:
        data = json.load(f)
    creds = data["credentials"][0]
    return {"api_key": creds["api_key"], "api_secret": creds["api_secret"]}


def require(cond: bool, msg: str):
    if not cond:
        raise AssertionError(f"FAIL: {msg}")


# ── Event collector ─────────────────────────────────────────────────────


class EventCollector:
    """Collects WS events, allows waiting for specific events."""

    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self._waiters: List[asyncio.Event] = []

    def on_event(self, event: Dict[str, Any]) -> None:
        self.events.append(event)
        for w in self._waiters:
            w.set()

    async def wait_for(
        self,
        order_id: int,
        exec_type: str,
        timeout: float = 30.0,
    ) -> Optional[Dict[str, Any]]:
        """Wait until an event matching (order_id, exec_type) arrives."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for e in self.events:
                if e["i"] == order_id and e["x"] == exec_type:
                    return e
            waiter = asyncio.Event()
            self._waiters.append(waiter)
            try:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                await asyncio.wait_for(waiter.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            finally:
                self._waiters.remove(waiter)
        return None

    async def wait_for_symbol_exec(
        self,
        symbol: str,
        exec_type: str,
        client_prefix: str = CLIENT_ID_PREFIX,
        timeout: float = 30.0,
    ) -> Optional[Dict[str, Any]]:
        """Wait for any event matching (symbol, exec_type, client_prefix)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for e in self.events:
                if (e["s"] == symbol and e["x"] == exec_type
                        and e.get("c", "").startswith(client_prefix)):
                    return e
            waiter = asyncio.Event()
            self._waiters.append(waiter)
            try:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                await asyncio.wait_for(waiter.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            finally:
                self._waiters.remove(waiter)
        return None


# ── Helpers ─────────────────────────────────────────────────────────────


def _cid(tag: str) -> str:
    """Generate a unique clientOrderId for this test run."""
    return f"{CLIENT_ID_PREFIX}{tag}_{int(time.time()*1000) % 1_000_000}"


async def _cancel_test_orders(client: BinanceFuturesClient, symbol: str):
    """Cancel any open orders from this test run (by clientOrderId prefix)."""
    try:
        open_orders = await client.get_open_orders(symbol)
        for o in open_orders:
            cid = o.get("clientOrderId", "")
            if cid.startswith(CLIENT_ID_PREFIX):
                try:
                    await client.cancel_order(symbol, order_id=int(o["orderId"]))
                    logger.info("Cleanup: canceled order %s (%s)", o["orderId"], cid)
                except BinanceAPIError as exc:
                    logger.warning("Cleanup cancel failed for %s: %s", o["orderId"], exc)
    except Exception as exc:
        logger.warning("Cleanup get_open_orders failed: %s", exc)


async def _close_test_position(client: BinanceFuturesClient, symbol: str):
    """Close any position on the test symbol held by this account."""
    try:
        positions = await client.get_positions()
        for p in positions:
            if p["symbol"] == symbol:
                amt = float(p["positionAmt"])
                if amt == 0:
                    continue
                side = "SELL" if amt > 0 else "BUY"
                qty = str(abs(int(amt)))  # ADAUSDT qty is integer
                logger.info("Cleanup: closing %s position of %s %s", side, qty, symbol)
                await client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type="MARKET",
                    quantity=qty,
                    reduce_only=True,
                    new_client_order_id=_cid("cleanup"),
                )
                await asyncio.sleep(1)
    except Exception as exc:
        logger.warning("Cleanup close position failed: %s", exc)


# ── Tests ───────────────────────────────────────────────────────────────


async def run_tests():
    creds = load_credentials()
    client = BinanceFuturesClient(**creds)
    collector = EventCollector()
    persister = OrderEventPersister(db_root=TEST_DB_DIR)

    # Combined callback: persister + collector
    def on_order_event(event):
        persister.on_order_event(event)
        collector.on_event(event)

    ws = UserDataWS(
        client,
        on_order_event=on_order_event,
    )

    passed = 0
    failed = 0
    test_order_ids = []  # track for cleanup

    try:
        # ── Setup: connect WS ──────────────────────────────────────
        ws.start()
        logger.info("Waiting for WS connection...")
        for _ in range(100):
            if ws.connected:
                break
            await asyncio.sleep(0.1)
        require(ws.connected, "UserDataWS failed to connect within 10s")

        # Switch to live mode immediately (no REST sync in this test)
        ws.drain_buffer()
        logger.info("WS connected, live mode active")

        # Get current price
        ticker = await client.get_ticker_price(TEST_SYMBOL)
        market_price = float(ticker["price"])
        logger.info("%s market price: %.4f", TEST_SYMBOL, market_price)

        # ════════════════════════════════════════════════════════════
        # TEST 1: LIMIT BUY → NEW event
        # ════════════════════════════════════════════════════════════
        try:
            logger.info("\n[TEST 1] LIMIT BUY → NEW event")
            limit_price = f"{market_price * 0.90:.4f}"  # 10% below market
            cid_limit = _cid("limit_buy")

            result = await client.place_order(
                symbol=TEST_SYMBOL,
                side="BUY",
                order_type="LIMIT",
                quantity=TEST_QTY,
                price=limit_price,
                time_in_force="GTC",
                new_client_order_id=cid_limit,
            )
            limit_order_id = int(result["orderId"])
            test_order_ids.append(limit_order_id)
            logger.info("Placed LIMIT BUY order=%d price=%s", limit_order_id, limit_price)

            event = await collector.wait_for(limit_order_id, "NEW", timeout=15)
            require(event is not None, "No NEW event for LIMIT BUY")
            require(event["X"] == "NEW", f"Expected status NEW, got {event['X']}")
            require(event["o"] == "LIMIT", f"Expected type LIMIT, got {event['o']}")

            # Verify persisted to DB
            order_db = persister.get_order_db_for_test(TEST_SYMBOL)
            rows = order_db.get_orders_in_window(
                event["T"] - 1000, event["T"] + 1000
            )
            found = [r for r in rows if r["order_id"] == limit_order_id]
            require(len(found) == 1, f"Expected 1 DB row, got {len(found)}")
            require(found[0]["execution_type"] == "NEW", "DB row exec_type mismatch")

            logger.info("  [PASS] LIMIT BUY → NEW event persisted")
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 1: %s", exc)
            failed += 1

        # ════════════════════════════════════════════════════════════
        # TEST 2: Cancel LIMIT BUY → CANCELED event
        # ════════════════════════════════════════════════════════════
        try:
            logger.info("\n[TEST 2] Cancel LIMIT BUY → CANCELED event")
            await client.cancel_order(TEST_SYMBOL, order_id=limit_order_id)
            logger.info("Canceled LIMIT BUY order=%d", limit_order_id)

            event = await collector.wait_for(limit_order_id, "CANCELED", timeout=15)
            require(event is not None, "No CANCELED event for LIMIT BUY")
            require(event["X"] == "CANCELED", f"Expected status CANCELED, got {event['X']}")

            # Verify in DB
            order_db = persister.get_order_db_for_test(TEST_SYMBOL)
            rows = order_db.get_orders_in_window(
                event["T"] - 1000, event["T"] + 1000
            )
            found = [r for r in rows if r["order_id"] == limit_order_id
                     and r["execution_type"] == "CANCELED"]
            require(len(found) == 1, f"Expected 1 CANCELED row, got {len(found)}")

            logger.info("  [PASS] LIMIT BUY cancel → CANCELED event persisted")
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 2: %s", exc)
            failed += 1

        # ════════════════════════════════════════════════════════════
        # TEST 3: MARKET BUY → TRADE (fill) event + user_trade
        # ════════════════════════════════════════════════════════════
        try:
            logger.info("\n[TEST 3] MARKET BUY → TRADE event + user_trade")
            cid_market = _cid("mkt_buy")

            result = await client.place_order(
                symbol=TEST_SYMBOL,
                side="BUY",
                order_type="MARKET",
                quantity=TEST_QTY,
                new_client_order_id=cid_market,
            )
            market_order_id = int(result["orderId"])
            test_order_ids.append(market_order_id)
            logger.info("Placed MARKET BUY order=%d", market_order_id)

            event = await collector.wait_for(market_order_id, "TRADE", timeout=15)
            require(event is not None, "No TRADE event for MARKET BUY")
            require(event["X"] in ("FILLED", "PARTIALLY_FILLED"),
                    f"Expected FILLED/PARTIALLY_FILLED, got {event['X']}")

            # Verify order_event in DB
            order_db = persister.get_order_db_for_test(TEST_SYMBOL)
            rows = order_db.get_orders_in_window(
                event["T"] - 1000, event["T"] + 1000
            )
            trade_rows = [r for r in rows if r["order_id"] == market_order_id
                          and r["execution_type"] == "TRADE"]
            require(len(trade_rows) >= 1, f"Expected ≥1 TRADE row, got {len(trade_rows)}")
            require(trade_rows[0]["commission"] > 0, "Commission should be > 0")

            # Verify user_trade in DB
            trade_db = persister.get_trade_db_for_test(TEST_SYMBOL)
            trades = trade_db.get_trades_in_window(
                event["T"] - 1000, event["T"] + 1000
            )
            my_trades = [t for t in trades if t["order_id"] == market_order_id]
            require(len(my_trades) >= 1, f"Expected ≥1 user_trade, got {len(my_trades)}")
            require(my_trades[0]["price"] > 0, "Trade price should be > 0")
            require(my_trades[0]["qty"] > 0, "Trade qty should be > 0")
            require(my_trades[0]["is_buyer"] == 1, "Should be buyer for BUY side")

            fill_price = my_trades[0]["price"]
            logger.info("  [PASS] MARKET BUY → TRADE event persisted (fill@%.4f)", fill_price)
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 3: %s", exc)
            failed += 1

        # ════════════════════════════════════════════════════════════
        # TEST 4: LIMIT fill — place LIMIT SELL at market to fill
        # ════════════════════════════════════════════════════════════
        # We already have a LONG position from TEST 3.
        # Place a LIMIT SELL reduce-only at or below market price to fill instantly.
        try:
            logger.info("\n[TEST 4] LIMIT SELL fill → TRADE event")
            # Refresh price
            ticker = await client.get_ticker_price(TEST_SYMBOL)
            current_price = float(ticker["price"])
            # Place limit at market - slight buffer to fill as taker
            limit_sell_price = f"{current_price * 0.995:.4f}"
            cid_limit_fill = _cid("limit_fill")

            result = await client.place_order(
                symbol=TEST_SYMBOL,
                side="SELL",
                order_type="LIMIT",
                quantity=TEST_QTY,
                price=limit_sell_price,
                time_in_force="GTC",
                reduce_only=True,
                new_client_order_id=cid_limit_fill,
            )
            limit_fill_id = int(result["orderId"])
            test_order_ids.append(limit_fill_id)
            logger.info("Placed LIMIT SELL reduce-only order=%d price=%s",
                        limit_fill_id, limit_sell_price)

            event = await collector.wait_for(limit_fill_id, "TRADE", timeout=15)
            require(event is not None, "No TRADE event for LIMIT SELL fill")
            require(event["X"] in ("FILLED", "PARTIALLY_FILLED"),
                    f"Expected FILLED status, got {event['X']}")

            # Verify user_trade
            trade_db = persister.get_trade_db_for_test(TEST_SYMBOL)
            trades = trade_db.get_trades_in_window(
                event["T"] - 1000, event["T"] + 1000
            )
            my_trades = [t for t in trades if t["order_id"] == limit_fill_id]
            require(len(my_trades) >= 1, f"Expected ≥1 user_trade, got {len(my_trades)}")
            require(my_trades[0]["is_buyer"] == 0, "Should not be buyer for SELL")

            logger.info("  [PASS] LIMIT SELL fill → TRADE event persisted")
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 4: %s", exc)
            failed += 1

        # Position should be flat now. Re-open for STOP tests.
        await asyncio.sleep(0.5)

        # ════════════════════════════════════════════════════════════
        # TEST 5: Re-open position + STOP (algo) order placement
        # Note: STOP/STOP_MARKET use the algo order API (different
        # endpoint). They generate STRATEGY_UPDATE WS events, not
        # ORDER_TRADE_UPDATE, so we verify via REST response only.
        # ════════════════════════════════════════════════════════════
        try:
            logger.info("\n[TEST 5] STOP (algo) SELL → place + verify")
            # Re-enter: MARKET BUY
            cid_reenter = _cid("reenter")
            result = await client.place_order(
                symbol=TEST_SYMBOL,
                side="BUY",
                order_type="MARKET",
                quantity=TEST_QTY,
                new_client_order_id=cid_reenter,
            )
            reenter_id = int(result["orderId"])
            test_order_ids.append(reenter_id)
            await collector.wait_for(reenter_id, "TRADE", timeout=15)
            logger.info("Re-entered LONG position")

            # Get fill price to set SL
            ticker = await client.get_ticker_price(TEST_SYMBOL)
            entry_price = float(ticker["price"])

            # Place algo STOP_MARKET order (conditional SL)
            stop_trigger = f"{entry_price * 0.93:.4f}"
            cid_stop = _cid("stop_sl")

            result = await client.place_algo_order(
                symbol=TEST_SYMBOL,
                side="SELL",
                order_type="STOP_MARKET",
                quantity=TEST_QTY,
                trigger_price=stop_trigger,
                reduce_only=True,
                client_algo_id=cid_stop,
            )
            stop_algo_id = int(result["algoId"])
            logger.info("Placed algo STOP_MARKET SELL algoId=%d trigger=%s",
                        stop_algo_id, stop_trigger)

            require(result.get("algoStatus") == "NEW",
                    f"Expected algoStatus NEW, got {result.get('algoStatus')}")
            require(result.get("symbol") == TEST_SYMBOL,
                    f"Expected symbol {TEST_SYMBOL}, got {result.get('symbol')}")

            logger.info("  [PASS] Algo STOP_MARKET placed, algoId=%d", stop_algo_id)
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 5: %s", exc)
            failed += 1

        # ════════════════════════════════════════════════════════════
        # TEST 6: Cancel algo STOP order
        # ════════════════════════════════════════════════════════════
        try:
            logger.info("\n[TEST 6] Cancel algo STOP → verify")
            await client.cancel_algo_order(algo_id=stop_algo_id)
            logger.info("Canceled algo STOP algoId=%d", stop_algo_id)

            logger.info("  [PASS] Algo STOP cancel succeeded")
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 6: %s", exc)
            failed += 1

        # ════════════════════════════════════════════════════════════
        # TEST 7: EXPIRED reduce-only order
        # Place reduce-only LIMIT SELL far above market, then close
        # position with MARKET → reduce-only gets EXPIRED
        # ════════════════════════════════════════════════════════════
        try:
            logger.info("\n[TEST 7] EXPIRED reduce-only order")
            ticker = await client.get_ticker_price(TEST_SYMBOL)
            current_price = float(ticker["price"])

            # Place reduce-only LIMIT SELL far above market (won't fill)
            ro_price = f"{current_price * 1.10:.4f}"  # 10% above
            cid_ro = _cid("reduce_only")

            result = await client.place_order(
                symbol=TEST_SYMBOL,
                side="SELL",
                order_type="LIMIT",
                quantity=TEST_QTY,
                price=ro_price,
                time_in_force="GTC",
                reduce_only=True,
                new_client_order_id=cid_ro,
            )
            ro_order_id = int(result["orderId"])
            test_order_ids.append(ro_order_id)
            logger.info("Placed reduce-only LIMIT SELL order=%d price=%s",
                        ro_order_id, ro_price)

            # Wait for NEW event
            event = await collector.wait_for(ro_order_id, "NEW", timeout=15)
            require(event is not None, "No NEW event for reduce-only order")

            # Close position with MARKET SELL → should expire the reduce-only
            cid_close = _cid("close_pos")
            close_result = await client.place_order(
                symbol=TEST_SYMBOL,
                side="SELL",
                order_type="MARKET",
                quantity=TEST_QTY,
                reduce_only=True,
                new_client_order_id=cid_close,
            )
            close_id = int(close_result["orderId"])
            test_order_ids.append(close_id)
            logger.info("Closed position with MARKET SELL order=%d", close_id)

            # Wait for EXPIRED event on the reduce-only limit order
            expired_event = await collector.wait_for(ro_order_id, "EXPIRED", timeout=15)
            require(expired_event is not None,
                    "No EXPIRED event for reduce-only order after position close")
            require(expired_event["X"] == "EXPIRED",
                    f"Expected EXPIRED status, got {expired_event['X']}")

            # Verify in DB
            order_db = persister.get_order_db_for_test(TEST_SYMBOL)
            rows = order_db.get_orders_in_window(
                expired_event["T"] - 1000, expired_event["T"] + 1000
            )
            found = [r for r in rows if r["order_id"] == ro_order_id
                     and r["execution_type"] == "EXPIRED"]
            require(len(found) == 1, "Expected 1 EXPIRED row in DB")
            require(found[0]["is_reduce_only"] == 1, "DB is_reduce_only should be 1")

            logger.info("  [PASS] EXPIRED reduce-only → event persisted")
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 7: %s", exc)
            failed += 1

        # ════════════════════════════════════════════════════════════
        # TEST 8: Algo STOP (second round) → place + cancel
        # Verifies algo order round-trip while a position is open.
        # ════════════════════════════════════════════════════════════
        try:
            logger.info("\n[TEST 8] Algo STOP round-trip → place + cancel")
            # Need a position first
            cid_pos8 = _cid("pos_for_sm")
            result = await client.place_order(
                symbol=TEST_SYMBOL,
                side="BUY",
                order_type="MARKET",
                quantity=TEST_QTY,
                new_client_order_id=cid_pos8,
            )
            pos8_id = int(result["orderId"])
            test_order_ids.append(pos8_id)
            await collector.wait_for(pos8_id, "TRADE", timeout=15)

            ticker = await client.get_ticker_price(TEST_SYMBOL)
            entry_price = float(ticker["price"])
            stop_trigger = f"{entry_price * 0.93:.4f}"
            cid_sm = _cid("stop_market")

            result = await client.place_algo_order(
                symbol=TEST_SYMBOL,
                side="SELL",
                order_type="STOP_MARKET",
                quantity=TEST_QTY,
                trigger_price=stop_trigger,
                reduce_only=True,
                client_algo_id=cid_sm,
            )
            sm_algo_id = int(result["algoId"])
            logger.info("Placed algo STOP_MARKET SELL algoId=%d trigger=%s",
                        sm_algo_id, stop_trigger)

            require(result.get("algoStatus") == "NEW",
                    f"Expected algoStatus NEW, got {result.get('algoStatus')}")

            # Cancel it
            await client.cancel_algo_order(algo_id=sm_algo_id)
            logger.info("Canceled algo STOP_MARKET algoId=%d", sm_algo_id)

            logger.info("  [PASS] Algo STOP round-trip (place + cancel) succeeded")
            passed += 1
        except Exception as exc:
            logger.error("  [FAIL] TEST 8: %s", exc)
            failed += 1

    finally:
        # ── Cleanup ────────────────────────────────────────────────
        logger.info("\n--- CLEANUP ---")
        await _cancel_test_orders(client, TEST_SYMBOL)
        await _close_test_position(client, TEST_SYMBOL)
        await asyncio.sleep(1)
        await ws.stop()
        persister.close()
        await client.close()
        logger.info("Cleanup complete")

    # ── Summary ────────────────────────────────────────────────────
    total = passed + failed
    print(f"\n{'='*60}")
    print(f"RESULTS: {passed} passed, {failed} failed out of {total} tests")
    print(f"{'='*60}")
    return failed


async def main():
    # Clean up test DB directory
    if os.path.exists(TEST_DB_DIR):
        shutil.rmtree(TEST_DB_DIR)
    os.makedirs(TEST_DB_DIR, exist_ok=True)

    try:
        failed = await run_tests()
    finally:
        # Clean up test DBs
        if os.path.exists(TEST_DB_DIR):
            shutil.rmtree(TEST_DB_DIR)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
