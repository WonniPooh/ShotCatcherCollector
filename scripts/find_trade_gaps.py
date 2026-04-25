#!/usr/bin/env python3
"""
Scan a symbol's trades.db for gaps larger than a threshold and register
them as rest_gap rows so the collector will backfill them on next run.

Usage:
    python collector/scripts/find_trade_gaps.py RIVERUSDT --min-gap 60
    python collector/scripts/find_trade_gaps.py all --min-gap 60 --dry-run
    python collector/scripts/find_trade_gaps.py BTCUSDT --min-gap 300 --dry-run
    python collector/scripts/find_trade_gaps.py BTCUSDT --min-gap 120 --db-root /path/to/db_files

Arguments:
    SYMBOL          Trading pair (e.g. BTCUSDT), or "all" for every symbol in db-root
    --min-gap SEC   Minimum gap size in seconds to register (default: 60)
    --db-root DIR   Root directory for per-symbol DBs (default: db_files)
    --dry-run       Show gaps without inserting into DB
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from repo root: python collector/scripts/find_trade_gaps.py
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "BinanceDataManagers"))

from trades_manager.trades_db_manager import AggTradeDB


def find_gaps(db: AggTradeDB, min_gap_ms: int) -> list[tuple[int, int, int]]:
    """Find consecutive-trade gaps larger than *min_gap_ms*.

    Returns list of (prev_ts, next_ts, gap_ms) sorted ascending.
    """
    rows = db.conn.execute(
        """
        SELECT trade_ts_ms,
               LEAD(trade_ts_ms) OVER (ORDER BY trade_ts_ms) AS next_ts
        FROM agg_trade
        """
    ).fetchall()

    gaps = []
    for ts, next_ts in rows:
        if next_ts is None:
            continue
        gap = next_ts - ts
        if gap >= min_gap_ms:
            gaps.append((ts, next_ts, gap))
    return gaps


def process_symbol(symbol: str, db_root: Path, min_gap_ms: int, dry_run: bool) -> None:
    db_path = db_root / symbol / "trades.db"
    if not db_path.exists():
        print(f"  [SKIP] {symbol}: trades.db not found")
        return

    db = AggTradeDB(str(db_path))
    try:
        existing = {(gs, gf) for _, gs, gf in db.list_gaps()}
        gaps = find_gaps(db, min_gap_ms)

        if not gaps:
            print(f"  {symbol}: no gaps found")
            return

        min_gap_s = min_gap_ms // 1000
        print(f"  {symbol}: {len(gaps)} gap(s) >= {min_gap_s}s")
        registered = 0
        skipped = 0
        for prev_ts, next_ts, gap_ms in gaps:
            gap_s = gap_ms / 1000
            tag = ""
            gap_start = prev_ts + 1
            gap_end = next_ts
            if (gap_start, gap_end) in existing:
                tag = " [already registered]"
                skipped += 1
            elif not dry_run:
                db.open_gap(gap_start, gap_end)
                registered += 1
                tag = " [REGISTERED]"

            h = int(gap_s // 3600)
            m = int((gap_s % 3600) // 60)
            s = int(gap_s % 60)
            duration = f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"
            print(f"    {prev_ts} → {next_ts}  ({duration}){tag}")

        if dry_run:
            print(f"    → dry run: {len(gaps)} found, {skipped} already registered")
        else:
            print(f"    → {registered} registered, {skipped} already existed")
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find and register trade data gaps in a symbol DB.")
    parser.add_argument("symbol",
                        help='Trading pair (e.g. BTCUSDT), or "all" for every symbol in db-root')
    parser.add_argument("--min-gap", type=int, default=60,
                        help="Minimum gap size in seconds (default: 60)")
    parser.add_argument("--db-root", default="db_files",
                        help="Root directory for per-symbol DBs (default: db_files)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show gaps without inserting into DB")
    args = parser.parse_args()

    db_root = Path(args.db_root)
    min_gap_ms = args.min_gap * 1000

    if args.symbol.lower() == "all":
        symbols = sorted(
            p.name for p in db_root.iterdir()
            if p.is_dir() and (p / "trades.db").exists()
        )
        if not symbols:
            print(f"No symbol directories with trades.db found in {db_root}")
            sys.exit(1)
        print(f"Scanning {len(symbols)} symbol(s) for gaps >= {args.min_gap}s "
              f"({'dry run' if args.dry_run else 'will register'}):\n")
        for symbol in symbols:
            process_symbol(symbol, db_root, min_gap_ms, args.dry_run)
    else:
        symbol = args.symbol.upper()
        db_path = db_root / symbol / "trades.db"
        if not db_path.exists():
            print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
            sys.exit(1)
        print(f"Scanning {symbol} for gaps >= {args.min_gap}s "
              f"({'dry run' if args.dry_run else 'will register'}):\n")
        process_symbol(symbol, db_root, min_gap_ms, args.dry_run)


if __name__ == "__main__":
    main()
