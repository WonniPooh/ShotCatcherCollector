# test_order_event_persister.py
"""
Unit tests for OrderEventPersister and WS event mappers.

Tests cover:
  - WS short-key → DB long-key mapping (orders and trades)
  - NEW order creates order_event row
  - TRADE event creates both order_event + user_trade rows
  - CANCELED event creates order_event, no trade row
  - AMENDMENT execution type stored correctly
  - Batch insert from drain_buffer replay
  - Idempotent upsert — same event twice doesn't duplicate
  - Unknown symbol auto-creates DB
  - Edge: trade_id=0 on TRADE event skips trade row
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "data_manager"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "data_manager" / "order_data_manager"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "data_manager" / "user_trades_manager"))

from order_event_persister import OrderEventPersister
from order_data_manager.order_data_loader import ws_order_event_to_row
from user_trades_manager.user_trades_loader import ws_event_to_trade_row

TEST_DB_DIR = "logs/test_order_event_persister"


def _setup():
    if os.path.exists(TEST_DB_DIR):
        shutil.rmtree(TEST_DB_DIR)
    os.makedirs(TEST_DB_DIR, exist_ok=True)


def _teardown():
    if os.path.exists(TEST_DB_DIR):
        shutil.rmtree(TEST_DB_DIR)


def _make_event(
    symbol="BTCUSDT",
    order_id=1000001,
    exec_type="NEW",
    status="NEW",
    side="BUY",
    order_type="LIMIT",
    price="50000",
    qty="0.001",
    avg_price="0",
    stop_price="0",
    last_fill_price="0",
    last_fill_qty="0",
    cum_filled_qty="0",
    commission="0",
    commission_asset="",
    trade_id=0,
    realized_pnl="0",
    is_maker=False,
    is_reduce_only=False,
    event_time=1700000000000,
    transaction_time=1700000000000,
    position_side="BOTH",
    time_in_force="GTC",
    client_order_id="test_order_1",
) -> dict:
    """Build a parsed WS ORDER_TRADE_UPDATE event dict (short Binance keys)."""
    return {
        "s": symbol,
        "c": client_order_id,
        "S": side,
        "o": order_type,
        "f": time_in_force,
        "q": qty,
        "p": price,
        "ap": avg_price,
        "sp": stop_price,
        "x": exec_type,
        "X": status,
        "i": order_id,
        "l": last_fill_qty,
        "z": cum_filled_qty,
        "L": last_fill_price,
        "n": commission,
        "N": commission_asset,
        "T": transaction_time,
        "t": trade_id,
        "rp": realized_pnl,
        "m": is_maker,
        "R": is_reduce_only,
        "ps": position_side,
        "E": event_time,
    }


# ── Mapper tests ─────────────────────────────────────────────────────────


def test_ws_order_event_to_row():
    """WS short-key dict maps correctly to DB long-key row."""
    event = _make_event(
        symbol="ETHUSDT", order_id=42, exec_type="NEW", status="NEW",
        price="3000.5", qty="1.5", stop_price="2900",
        is_reduce_only=True, time_in_force="IOC",
    )
    row = ws_order_event_to_row(event)

    assert row["order_id"] == 42
    assert row["symbol"] == "ETHUSDT"
    assert row["execution_type"] == "NEW"
    assert row["order_status"] == "NEW"
    assert row["order_price"] == 3000.5
    assert row["order_qty"] == 1.5
    assert row["stop_price"] == 2900.0
    assert row["is_reduce_only"] == 1
    assert row["time_in_force"] == "IOC"
    assert row["trade_id"] == 0
    assert row["last_fill_price"] == 0.0
    print("  [OK] test_ws_order_event_to_row")


def test_ws_event_to_trade_row():
    """WS TRADE event maps correctly to UserTradeDB row."""
    event = _make_event(
        symbol="BTCUSDT", order_id=100, exec_type="TRADE", status="FILLED",
        price="50000", last_fill_price="49999.5", last_fill_qty="0.001",
        commission="0.02", commission_asset="USDT", trade_id=500001,
        realized_pnl="5.25", is_maker=True, side="SELL",
        transaction_time=1700000001000,
    )
    row = ws_event_to_trade_row(event)

    assert row["trade_id"] == 500001
    assert row["order_id"] == 100
    assert row["symbol"] == "BTCUSDT"
    assert row["side"] == "SELL"
    assert row["price"] == 49999.5
    assert row["qty"] == 0.001
    assert abs(row["quote_qty"] - 49999.5 * 0.001) < 0.001
    assert row["commission"] == 0.02
    assert row["commission_asset"] == "USDT"
    assert row["realized_pnl"] == 5.25
    assert row["is_maker"] == 1
    assert row["is_buyer"] == 0  # SELL side
    assert row["trade_time_ms"] == 1700000001000
    print("  [OK] test_ws_event_to_trade_row")


def test_ws_event_to_trade_row_buy():
    """BUY side → is_buyer=1."""
    event = _make_event(side="BUY", exec_type="TRADE", trade_id=1)
    row = ws_event_to_trade_row(event)
    assert row["is_buyer"] == 1
    print("  [OK] test_ws_event_to_trade_row_buy")


# ── Persister tests ──────────────────────────────────────────────────────


def test_persister_new_order():
    """NEW order creates a single order_event row, no trade row."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        event = _make_event(exec_type="NEW", status="NEW")
        p.on_order_event(event)

        order_db = p.get_order_db_for_test("BTCUSDT")
        rows = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(rows) == 1
        assert rows[0]["order_id"] == 1000001
        assert rows[0]["execution_type"] == "NEW"
        assert rows[0]["order_status"] == "NEW"

        trade_db = p.get_trade_db_for_test("BTCUSDT")
        trades = trade_db.get_trades_in_window(0, 2_000_000_000_000)
        assert len(trades) == 0

        p.close()
        print("  [OK] test_persister_new_order")
    finally:
        _teardown()


def test_persister_fill():
    """TRADE event creates both order_event and user_trade rows."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        event = _make_event(
            exec_type="TRADE", status="FILLED",
            last_fill_price="50000", last_fill_qty="0.001",
            cum_filled_qty="0.001", avg_price="50000",
            commission="0.02", commission_asset="USDT",
            trade_id=500001, realized_pnl="10.5",
        )
        p.on_order_event(event)

        order_db = p.get_order_db_for_test("BTCUSDT")
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 1
        assert orders[0]["execution_type"] == "TRADE"
        assert orders[0]["commission"] == 0.02

        trade_db = p.get_trade_db_for_test("BTCUSDT")
        trades = trade_db.get_trades_in_window(0, 2_000_000_000_000)
        assert len(trades) == 1
        assert trades[0]["trade_id"] == 500001
        assert trades[0]["price"] == 50000.0
        assert trades[0]["qty"] == 0.001
        assert trades[0]["realized_pnl"] == 10.5

        p.close()
        print("  [OK] test_persister_fill")
    finally:
        _teardown()


def test_persister_cancel():
    """CANCELED event creates order_event, no trade row."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        event = _make_event(exec_type="CANCELED", status="CANCELED")
        p.on_order_event(event)

        order_db = p.get_order_db_for_test("BTCUSDT")
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 1
        assert orders[0]["execution_type"] == "CANCELED"

        trade_db = p.get_trade_db_for_test("BTCUSDT")
        trades = trade_db.get_trades_in_window(0, 2_000_000_000_000)
        assert len(trades) == 0

        p.close()
        print("  [OK] test_persister_cancel")
    finally:
        _teardown()


def test_persister_amendment():
    """AMENDMENT execution type stored correctly in order_event."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        event = _make_event(
            exec_type="AMENDMENT", status="NEW",
            price="51000", qty="0.002",
            transaction_time=1700000002000,
        )
        p.on_order_event(event)

        order_db = p.get_order_db_for_test("BTCUSDT")
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 1
        assert orders[0]["execution_type"] == "AMENDMENT"
        assert orders[0]["order_price"] == 51000.0

        p.close()
        print("  [OK] test_persister_amendment")
    finally:
        _teardown()


def test_persist_batch():
    """Batch insert from drain_buffer replay works."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        events = [
            _make_event(order_id=1, exec_type="NEW", status="NEW",
                        transaction_time=1700000000000),
            _make_event(order_id=1, exec_type="TRADE", status="PARTIALLY_FILLED",
                        last_fill_price="50000", last_fill_qty="0.0005",
                        cum_filled_qty="0.0005", trade_id=100,
                        transaction_time=1700000001000),
            _make_event(order_id=1, exec_type="TRADE", status="FILLED",
                        last_fill_price="50000", last_fill_qty="0.0005",
                        cum_filled_qty="0.001", trade_id=101,
                        transaction_time=1700000002000),
            _make_event(order_id=2, exec_type="NEW", status="NEW",
                        symbol="ETHUSDT",
                        transaction_time=1700000001500),
        ]
        p.persist_batch(events)

        # BTCUSDT: 3 order events (NEW + 2 TRADE), 2 trade rows
        btc_orders = p.get_order_db_for_test("BTCUSDT").get_orders_in_window(0, 2_000_000_000_000)
        assert len(btc_orders) == 3

        btc_trades = p.get_trade_db_for_test("BTCUSDT").get_trades_in_window(0, 2_000_000_000_000)
        assert len(btc_trades) == 2

        # ETHUSDT: 1 order event, 0 trades
        eth_orders = p.get_order_db_for_test("ETHUSDT").get_orders_in_window(0, 2_000_000_000_000)
        assert len(eth_orders) == 1

        p.close()
        print("  [OK] test_persist_batch")
    finally:
        _teardown()


def test_persister_idempotent():
    """Same event inserted twice → no duplicate, fields updated."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)

        # First insert: NEW
        event = _make_event(exec_type="NEW", status="NEW", commission="0")
        p.on_order_event(event)

        # Second insert: same (order_id, execution_type, transaction_time)
        # but with updated commission
        event2 = _make_event(exec_type="NEW", status="NEW", commission="0.05")
        p.on_order_event(event2)

        order_db = p.get_order_db_for_test("BTCUSDT")
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 1  # no duplicate
        assert orders[0]["commission"] == 0.05  # updated value

        p.close()
        print("  [OK] test_persister_idempotent")
    finally:
        _teardown()


def test_persister_unknown_symbol():
    """Event for unknown symbol auto-creates DB directory and files."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        event = _make_event(symbol="XYZUSDT")
        p.on_order_event(event)

        sym_dir = Path(TEST_DB_DIR) / "XYZUSDT"
        assert sym_dir.is_dir()
        assert (sym_dir / "order_events.db").is_file()

        order_db = p.get_order_db_for_test("XYZUSDT")
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 1

        p.close()
        print("  [OK] test_persister_unknown_symbol")
    finally:
        _teardown()


def test_trade_id_zero_skips_trade_row():
    """TRADE event with trade_id=0 inserts order_event but skips user_trade."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        event = _make_event(
            exec_type="TRADE", status="FILLED",
            trade_id=0,  # edge case
        )
        p.on_order_event(event)

        order_db = p.get_order_db_for_test("BTCUSDT")
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 1

        trade_db = p.get_trade_db_for_test("BTCUSDT")
        trades = trade_db.get_trades_in_window(0, 2_000_000_000_000)
        assert len(trades) == 0  # skipped due to trade_id=0

        p.close()
        print("  [OK] test_trade_id_zero_skips_trade_row")
    finally:
        _teardown()


def test_persister_partial_fill_lifecycle():
    """Full order lifecycle: NEW → PARTIALLY_FILLED → FILLED."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)

        # NEW
        p.on_order_event(_make_event(
            order_id=42, exec_type="NEW", status="NEW",
            price="50000", qty="0.01",
            transaction_time=1700000000000,
        ))
        # PARTIALLY_FILLED
        p.on_order_event(_make_event(
            order_id=42, exec_type="TRADE", status="PARTIALLY_FILLED",
            price="50000", qty="0.01",
            last_fill_price="50000", last_fill_qty="0.005",
            cum_filled_qty="0.005", avg_price="50000",
            commission="0.01", commission_asset="USDT",
            trade_id=1001, realized_pnl="2.5",
            transaction_time=1700000001000,
        ))
        # FILLED
        p.on_order_event(_make_event(
            order_id=42, exec_type="TRADE", status="FILLED",
            price="50000", qty="0.01",
            last_fill_price="50001", last_fill_qty="0.005",
            cum_filled_qty="0.01", avg_price="50000.5",
            commission="0.01", commission_asset="USDT",
            trade_id=1002, realized_pnl="2.5",
            transaction_time=1700000002000,
        ))

        order_db = p.get_order_db_for_test("BTCUSDT")
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 3  # NEW, TRADE, TRADE

        trade_db = p.get_trade_db_for_test("BTCUSDT")
        trades = trade_db.get_trades_in_window(0, 2_000_000_000_000)
        assert len(trades) == 2
        assert trades[0]["trade_id"] == 1001
        assert trades[1]["trade_id"] == 1002
        assert trades[0]["qty"] == 0.005
        assert trades[1]["qty"] == 0.005

        p.close()
        print("  [OK] test_persister_partial_fill_lifecycle")
    finally:
        _teardown()


def test_persister_multiple_symbols():
    """Events for different symbols create separate DBs."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)

        p.on_order_event(_make_event(symbol="BTCUSDT", order_id=1))
        p.on_order_event(_make_event(symbol="ETHUSDT", order_id=2))
        p.on_order_event(_make_event(symbol="ADAUSDT", order_id=3))

        for sym in ("BTCUSDT", "ETHUSDT", "ADAUSDT"):
            db = p.get_order_db_for_test(sym)
            rows = db.get_orders_in_window(0, 2_000_000_000_000)
            assert len(rows) == 1
            assert rows[0]["symbol"] == sym

        p.close()
        print("  [OK] test_persister_multiple_symbols")
    finally:
        _teardown()


def test_persister_missing_symbol_skips():
    """Event with empty symbol is skipped."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        event = _make_event(symbol="")
        p.on_order_event(event)

        # No DB should be created for empty symbol
        assert len(list(Path(TEST_DB_DIR).iterdir())) == 0

        p.close()
        print("  [OK] test_persister_missing_symbol_skips")
    finally:
        _teardown()


def test_amendment_ws_event_stores_in_order_db():
    """AMENDMENT WS events write to order_amendment table via in-memory price tracking."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)

        # Mark symbol as synced so amendments write directly
        p._synced_symbols.add("BTCUSDT")

        # NEW order → sets price tracker
        p.on_order_event(_make_event(
            order_id=9000001, exec_type="NEW", status="NEW",
            price="50000", qty="0.001",
            transaction_time=1700000000000,
        ))

        # First AMENDMENT → should write with price_before=50000
        p.on_order_event(_make_event(
            order_id=9000001, exec_type="AMENDMENT", status="NEW",
            price="51000", qty="0.001",
            transaction_time=1700000010000,
        ))

        # Second AMENDMENT → should write with price_before=51000
        p.on_order_event(_make_event(
            order_id=9000001, exec_type="AMENDMENT", status="NEW",
            price="52000", qty="0.002",
            transaction_time=1700000020000,
        ))

        order_db = p.get_order_db_for_test("BTCUSDT")
        amendments = order_db.get_amendments_by_order(9000001)
        assert len(amendments) == 2, f"expected 2 amendments, got {len(amendments)}"
        assert amendments[0]["price_before"] == 50000.0
        assert amendments[0]["price_after"] == 51000.0
        assert amendments[1]["price_before"] == 51000.0
        assert amendments[1]["price_after"] == 52000.0
        assert amendments[1]["qty_before"] == 0.001
        assert amendments[1]["qty_after"] == 0.002

        # Verify no separate amendment DB file was created
        sym_dir = Path(TEST_DB_DIR) / "BTCUSDT"
        assert not (sym_dir / "order_amendments.db").exists(), \
            "order_amendments.db should NOT exist — amendments go into order_events.db"

        p.close()
        print("  [OK] test_amendment_ws_event_stores_in_order_db")
    finally:
        _teardown()


def test_amendment_caching_and_flush():
    """Pre-sync AMENDMENT events are cached, then flushed after REST sync."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)

        # NOT synced yet — amendments should be cached

        # NEW order → price tracker set
        p.on_order_event(_make_event(
            order_id=42, exec_type="NEW", status="NEW",
            price="100", qty="1.0",
            transaction_time=1700000000000,
        ))

        # AMENDMENT while not synced → cached, not written
        p.on_order_event(_make_event(
            order_id=42, exec_type="AMENDMENT", status="NEW",
            price="101", qty="1.0",
            transaction_time=1700000001000,
        ))

        order_db = p.get_order_db_for_test("BTCUSDT")
        amendments = order_db.get_amendments_by_order(42)
        assert len(amendments) == 0, "should be 0 before flush"
        assert len(p._cached_amendments) == 1

        # Simulate REST sync completing — flush
        written = p.flush_cached_amendments("BTCUSDT")
        assert written == 1

        amendments = order_db.get_amendments_by_order(42)
        assert len(amendments) == 1
        assert amendments[0]["price_before"] == 100.0
        assert amendments[0]["price_after"] == 101.0

        # After flush, new amendments go directly to DB
        assert "BTCUSDT" in p._synced_symbols
        p.on_order_event(_make_event(
            order_id=42, exec_type="AMENDMENT", status="NEW",
            price="102", qty="1.0",
            transaction_time=1700000002000,
        ))
        amendments = order_db.get_amendments_by_order(42)
        assert len(amendments) == 2
        assert amendments[1]["price_before"] == 101.0
        assert amendments[1]["price_after"] == 102.0

        p.close()
        print("  [OK] test_amendment_caching_and_flush")
    finally:
        _teardown()


def test_amendment_dedup_ws_vs_rest():
    """WS and REST amendments for same (order_id, time_ms) don't duplicate."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        p._synced_symbols.add("BTCUSDT")

        # NEW
        p.on_order_event(_make_event(
            order_id=42, exec_type="NEW", status="NEW",
            price="100", qty="1.0",
            transaction_time=1700000000000,
        ))

        # WS amendment
        p.on_order_event(_make_event(
            order_id=42, exec_type="AMENDMENT", status="NEW",
            price="101", qty="1.0",
            transaction_time=1700000001000,
        ))

        # REST amendment with same time_ms (simulating REST sync)
        order_db = p.get_order_db_for_test("BTCUSDT")
        order_db.insert_amendment_rows([{
            "amendment_id": 8001,
            "order_id": 42,
            "symbol": "BTCUSDT",
            "client_order_id": "test",
            "time_ms": 1700000001000,
            "price_before": 100.0,
            "price_after": 101.0,
            "qty_before": 1.0,
            "qty_after": 1.0,
            "amendment_count": 1,
        }])

        amendments = order_db.get_amendments_by_order(42)
        assert len(amendments) == 1, f"expected 1 (deduped), got {len(amendments)}"

        p.close()
        print("  [OK] test_amendment_dedup_ws_vs_rest")
    finally:
        _teardown()


def test_amendment_table_coexists_with_order_events():
    """Amendment and order_event tables live in same DB, queryable independently."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)

        # Insert order event
        p.on_order_event(_make_event(
            order_id=42, exec_type="NEW", status="NEW",
            transaction_time=1700000000000,
        ))

        # Insert amendment directly into the same DB
        order_db = p.get_order_db_for_test("BTCUSDT")
        order_db.insert_amendment_rows([{
            "amendment_id": 9001,
            "order_id": 42,
            "symbol": "BTCUSDT",
            "client_order_id": "test",
            "time_ms": 1700000005000,
            "price_before": 50000.0,
            "price_after": 51000.0,
            "qty_before": 0.001,
            "qty_after": 0.001,
            "amendment_count": 1,
        }])

        # Both tables queryable
        orders = order_db.get_orders_in_window(0, 2_000_000_000_000)
        assert len(orders) == 1
        assert orders[0]["order_id"] == 42

        amendments = order_db.get_amendments_by_order(42)
        assert len(amendments) == 1
        assert amendments[0]["price_after"] == 51000.0

        p.close()
        print("  [OK] test_amendment_table_coexists_with_order_events")
    finally:
        _teardown()


def test_tp_order_amendment_uses_stop_price():
    """TAKE_PROFIT_MARKET amendments use stop_price (sp), not order_price (p)."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        p._synced_symbols.add("BTCUSDT")

        # NEW TP order: p=0 (normal for TP_MARKET), sp=52000
        p.on_order_event(_make_event(
            order_id=7777, exec_type="NEW", status="NEW",
            order_type="TAKE_PROFIT_MARKET",
            price="0", stop_price="52000", qty="0.01",
            is_reduce_only=True,
            transaction_time=1700000000000,
        ))

        # First amendment: sp changes from 52000 → 51500
        p.on_order_event(_make_event(
            order_id=7777, exec_type="AMENDMENT", status="NEW",
            order_type="TAKE_PROFIT_MARKET",
            price="0", stop_price="51500", qty="0.01",
            is_reduce_only=True,
            transaction_time=1700000010000,
        ))

        # Second amendment: sp changes from 51500 → 51000
        p.on_order_event(_make_event(
            order_id=7777, exec_type="AMENDMENT", status="NEW",
            order_type="TAKE_PROFIT_MARKET",
            price="0", stop_price="51000", qty="0.01",
            is_reduce_only=True,
            transaction_time=1700000020000,
        ))

        order_db = p.get_order_db_for_test("BTCUSDT")
        amendments = order_db.get_amendments_by_order(7777)
        assert len(amendments) == 2, f"expected 2, got {len(amendments)}"

        # price_before/after must be stop_price values, NOT 0
        assert amendments[0]["price_before"] == 52000.0, \
            f"expected price_before=52000, got {amendments[0]['price_before']}"
        assert amendments[0]["price_after"] == 51500.0, \
            f"expected price_after=51500, got {amendments[0]['price_after']}"
        assert amendments[1]["price_before"] == 51500.0, \
            f"expected price_before=51500, got {amendments[1]['price_before']}"
        assert amendments[1]["price_after"] == 51000.0, \
            f"expected price_after=51000, got {amendments[1]['price_after']}"

        p.close()
        print("  [OK] test_tp_order_amendment_uses_stop_price")
    finally:
        _teardown()


def test_sl_order_amendment_uses_stop_price():
    """STOP_MARKET amendments use stop_price (sp), not order_price (p)."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        p._synced_symbols.add("BTCUSDT")

        # NEW SL order
        p.on_order_event(_make_event(
            order_id=8888, exec_type="NEW", status="NEW",
            order_type="STOP_MARKET",
            price="0", stop_price="48000", qty="0.01",
            is_reduce_only=True,
            transaction_time=1700000000000,
        ))

        # Amendment: sp 48000 → 48500
        p.on_order_event(_make_event(
            order_id=8888, exec_type="AMENDMENT", status="NEW",
            order_type="STOP_MARKET",
            price="0", stop_price="48500", qty="0.01",
            is_reduce_only=True,
            transaction_time=1700000010000,
        ))

        order_db = p.get_order_db_for_test("BTCUSDT")
        amendments = order_db.get_amendments_by_order(8888)
        assert len(amendments) == 1
        assert amendments[0]["price_before"] == 48000.0, \
            f"expected 48000, got {amendments[0]['price_before']}"
        assert amendments[0]["price_after"] == 48500.0, \
            f"expected 48500, got {amendments[0]['price_after']}"

        p.close()
        print("  [OK] test_sl_order_amendment_uses_stop_price")
    finally:
        _teardown()


def test_tp_amendment_caching_and_flush():
    """TP order amendments cached before sync use stop_price correctly on flush."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)

        # NOT synced — amendments go to cache

        # NEW TP order
        p.on_order_event(_make_event(
            order_id=9999, exec_type="NEW", status="NEW",
            order_type="TAKE_PROFIT_MARKET",
            price="0", stop_price="60000", qty="0.5",
            is_reduce_only=True,
            transaction_time=1700000000000,
        ))

        # Cached amendment
        p.on_order_event(_make_event(
            order_id=9999, exec_type="AMENDMENT", status="NEW",
            order_type="TAKE_PROFIT_MARKET",
            price="0", stop_price="59000", qty="0.5",
            is_reduce_only=True,
            transaction_time=1700000010000,
        ))

        assert len(p._cached_amendments) == 1

        # Flush — should use stop_price for price_before/after
        written = p.flush_cached_amendments("BTCUSDT")
        assert written == 1

        order_db = p.get_order_db_for_test("BTCUSDT")
        amendments = order_db.get_amendments_by_order(9999)
        assert len(amendments) == 1
        assert amendments[0]["price_before"] == 60000.0, \
            f"expected 60000, got {amendments[0]['price_before']}"
        assert amendments[0]["price_after"] == 59000.0, \
            f"expected 59000, got {amendments[0]['price_after']}"

        p.close()
        print("  [OK] test_tp_amendment_caching_and_flush")
    finally:
        _teardown()


def test_amendment_untracked_order_queues_repair():
    """Amendment for untracked order (missing NEW) queues a repair, not a silent drop."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        p._synced_symbols.add("BTCUSDT")

        # No NEW event — amendment for unknown order
        # Without rest_client, it should log a warning and not crash
        p.on_order_event(_make_event(
            order_id=55555, exec_type="AMENDMENT", status="NEW",
            price="42000", qty="0.01",
            transaction_time=1700000010000,
        ))

        # The amendment should be in the repair queue
        assert 55555 in p._repair_queue, "order should be in repair queue"
        assert len(p._repair_queue[55555]) == 1

        # No REST client → no async task spawned, but amendment is queued
        order_db = p.get_order_db_for_test("BTCUSDT")
        amendments = order_db.get_amendments_by_order(55555)
        assert len(amendments) == 0, "should NOT be written yet (no REST client to repair)"

        p.close()
        print("  [OK] test_amendment_untracked_order_queues_repair")
    finally:
        _teardown()


def test_amendment_untracked_order_multiple_queued():
    """Multiple amendments for same untracked order all queue up."""
    _setup()
    try:
        p = OrderEventPersister(db_root=TEST_DB_DIR)
        p._synced_symbols.add("BTCUSDT")

        # Two amendments for same untracked order
        p.on_order_event(_make_event(
            order_id=66666, exec_type="AMENDMENT", status="NEW",
            price="42000", qty="0.01",
            transaction_time=1700000010000,
        ))
        p.on_order_event(_make_event(
            order_id=66666, exec_type="AMENDMENT", status="NEW",
            price="43000", qty="0.01",
            transaction_time=1700000020000,
        ))

        assert 66666 in p._repair_queue
        assert len(p._repair_queue[66666]) == 2

        p.close()
        print("  [OK] test_amendment_untracked_order_multiple_queued")
    finally:
        _teardown()


# ── Main ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_ws_order_event_to_row,
        test_ws_event_to_trade_row,
        test_ws_event_to_trade_row_buy,
        test_persister_new_order,
        test_persister_fill,
        test_persister_cancel,
        test_persister_amendment,
        test_persist_batch,
        test_persister_idempotent,
        test_persister_unknown_symbol,
        test_trade_id_zero_skips_trade_row,
        test_persister_partial_fill_lifecycle,
        test_persister_multiple_symbols,
        test_persister_missing_symbol_skips,
        test_amendment_ws_event_stores_in_order_db,
        test_amendment_caching_and_flush,
        test_amendment_dedup_ws_vs_rest,
        test_amendment_table_coexists_with_order_events,
        test_tp_order_amendment_uses_stop_price,
        test_sl_order_amendment_uses_stop_price,
        test_tp_amendment_caching_and_flush,
        test_amendment_untracked_order_queues_repair,
        test_amendment_untracked_order_multiple_queued,
    ]

    print(f"Running {len(tests)} tests...\n")
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as exc:
            failed += 1
            print(f"  [FAIL] {test.__name__}: {exc}")

    print(f"\n{'='*60}")
    print(f"RESULTS: {passed} passed, {failed} failed out of {len(tests)} tests")
    print(f"{'='*60}")
    if failed:
        sys.exit(1)
