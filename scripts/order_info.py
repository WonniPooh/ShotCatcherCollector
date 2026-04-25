#!/usr/bin/env python3
"""
Load full order info from Binance exchange by order ID.

Usage:
    python3 collector/scripts/order_info.py VVVUSDT 123456789
    python3 collector/scripts/order_info.py VVVUSDT 123456789 --config config/collector.json
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from collector.binance_futures_client import BinanceFuturesClient
from collector.config import load_config


def ts_to_str(ms: int) -> str:
    if not ms:
        return "—"
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


def print_section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def print_kv(label: str, value: str, indent: int = 2) -> None:
    print(f"{' ' * indent}{label:<28} {value}")


async def run(symbol: str, order_id: int, config_path: str | None) -> None:
    cfg = load_config(config_path)
    client = BinanceFuturesClient(cfg.api_key, cfg.api_secret)

    try:
        # 1. Load order
        print_section(f"ORDER  {symbol}  #{order_id}")
        orders = await client.get_orders(symbol, order_id=order_id, limit=1)
        order = None
        for o in orders:
            if int(o["orderId"]) == order_id:
                order = o
                break

        if not order:
            print(f"  Order {order_id} not found for {symbol}")
            return

        print_kv("Order ID", str(order["orderId"]))
        print_kv("Client Order ID", order.get("clientOrderId", ""))
        print_kv("Symbol", order["symbol"])
        print_kv("Side", order["side"])
        print_kv("Position Side", order.get("positionSide", ""))
        print_kv("Type", order["type"])
        print_kv("Status", order["status"])
        print_kv("Time In Force", order.get("timeInForce", ""))
        print_kv("Price", order.get("price", ""))
        print_kv("Stop Price", order.get("stopPrice", ""))
        print_kv("Orig Qty", order.get("origQty", ""))
        print_kv("Executed Qty", order.get("executedQty", ""))
        print_kv("Avg Price", order.get("avgPrice", ""))
        print_kv("Cum Quote", order.get("cumQuote", ""))
        print_kv("Reduce Only", str(order.get("reduceOnly", "")))
        print_kv("Close Position", str(order.get("closePosition", "")))
        print_kv("Created", ts_to_str(order.get("time", 0)))
        print_kv("Updated", ts_to_str(order.get("updateTime", 0)))

        order_time = order.get("time", 0)

        # 2. Load amendments
        print_section("AMENDMENTS")
        amendments = await client.get_order_amendments(symbol, order_id=order_id)
        if not amendments:
            print("  No amendments found")
        else:
            print(f"  {len(amendments)} amendment(s)\n")
            for i, a in enumerate(amendments, 1):
                amend = a.get("amendment", {})
                print(f"  [{i}]  {ts_to_str(a.get('time', 0))}")
                print_kv("Price", f"{amend.get('price', {}).get('before', '?')} → {amend.get('price', {}).get('after', '?')}", 6)
                print_kv("Qty", f"{amend.get('qty', {}).get('before', '?')} → {amend.get('qty', {}).get('after', '?')}", 6)
                print_kv("Count", str(amend.get('count', '?')), 6)
                print()

        # 3. Load user trades for this order
        print_section("FILLS (USER TRADES)")
        # Query trades around the order time window
        if order_time:
            trades = await client.get_user_trades(
                symbol,
                start_time=order_time,
                limit=1000,
            )
        else:
            trades = await client.get_user_trades(symbol, limit=1000)

        order_trades = [t for t in trades if int(t["orderId"]) == order_id]

        if not order_trades:
            print("  No fills found")
        else:
            total_qty = 0.0
            total_quote = 0.0
            total_commission = 0.0
            total_pnl = 0.0

            print(f"  {len(order_trades)} fill(s)\n")
            print(f"  {'#':<4} {'Time':<28} {'Price':<14} {'Qty':<14} {'Quote':<14} {'Comm':<12} {'PnL':<12} {'Maker'}")
            print(f"  {'─'*4} {'─'*28} {'─'*14} {'─'*14} {'─'*14} {'─'*12} {'─'*12} {'─'*5}")

            for i, t in enumerate(order_trades, 1):
                price = t.get("price", "0")
                qty = t.get("qty", "0")
                quote_qty = t.get("quoteQty", "0")
                commission = t.get("commission", "0")
                rpnl = t.get("realizedPnl", "0")
                maker = "Yes" if t.get("maker") else "No"
                time_str = ts_to_str(t.get("time", 0))

                total_qty += float(qty)
                total_quote += float(quote_qty)
                total_commission += float(commission)
                total_pnl += float(rpnl)

                print(f"  {i:<4} {time_str:<28} {price:<14} {qty:<14} {quote_qty:<14} {commission:<12} {rpnl:<12} {maker}")

            print(f"\n  {'TOTAL':<4} {'':<28} {'':<14} {total_qty:<14.6f} {total_quote:<14.4f} {total_commission:<12.8f} {total_pnl:<12.4f}")
            if total_qty > 0:
                avg = total_quote / total_qty
                print_kv("Weighted Avg Price", f"{avg:.8f}")

    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load order info from Binance")
    parser.add_argument("symbol", help="Trading pair (e.g. VVVUSDT)")
    parser.add_argument("order_id", type=int, help="Binance order ID")
    parser.add_argument("--config", default=None, help="Path to collector config JSON")
    args = parser.parse_args()

    asyncio.run(run(args.symbol.upper(), args.order_id, args.config))


if __name__ == "__main__":
    main()
