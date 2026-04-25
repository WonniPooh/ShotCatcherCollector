# market_data_loader.py
"""
On-demand market data downloader for the controller.

When the chart UI requests candles/trades for a symbol that has no local data,
this module downloads ~1 week of 1-minute candles from the public Binance REST API
(no API key needed) and stores them in CandleDB.

Subsequent requests are served from SQLite. On next startup, only the gap
between the latest stored candle and now is downloaded (incremental).
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import aiohttp

logger = logging.getLogger("controller.market_data_loader")

# Binance Futures public klines endpoint (no API key needed)
_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
_BATCH_LIMIT = 1000  # max candles per request
_1M_MS = 60_000      # 1 minute in ms
_1W_MS = 7 * 24 * 60 * _1M_MS  # 1 week in ms

# Track in-flight downloads to avoid duplicate work
_loading: dict[str, asyncio.Task] = {}


def _db_dir_for_symbol(db_root: str, symbol: str) -> Path:
    d = Path(db_root) / symbol
    d.mkdir(parents=True, exist_ok=True)
    return d


async def _fetch_klines_batch(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    start_time: int,
    end_time: int,
    limit: int = _BATCH_LIMIT,
) -> list[list]:
    """Fetch one batch of klines from Binance public API."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    async with session.get(_KLINES_URL, params=params) as resp:
        if resp.status != 200:
            text = await resp.text()
            logger.error("Klines fetch failed %d: %s", resp.status, text[:200])
            return []
        return await resp.json()


async def download_klines_for_symbol(
    db_root: str,
    symbol: str,
    lookback_ms: int = _1W_MS,
) -> int:
    """
    Download 1m klines for `symbol` covering the last `lookback_ms` period.
    Incremental: if local data exists, only downloads the gap.
    Returns total candles inserted.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_manager"))
    from klines_db_manager import CandleDB

    sym_dir = _db_dir_for_symbol(db_root, symbol)
    db_path = sym_dir / f"{symbol}_1m.db"
    db = CandleDB(str(db_path))

    now_ms = int(time.time() * 1000)
    desired_start = now_ms - lookback_ms

    # Check existing data for incremental download
    latest = db.get_latest_candle_time()
    if latest and latest > desired_start:
        # Only fetch the gap: from latest+1min to now
        start_ms = latest + _1M_MS
        if start_ms >= now_ms:
            logger.info("%s: klines already up to date", symbol)
            db.close()
            return 0
        logger.info("%s: incremental kline download from %d", symbol, start_ms)
    else:
        start_ms = desired_start
        logger.info("%s: full kline download from %d (%.1f days back)",
                    symbol, start_ms, lookback_ms / (_1M_MS * 60 * 24))

    total_inserted = 0
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        cursor = start_ms
        while cursor < now_ms:
            batch_end = min(cursor + _BATCH_LIMIT * _1M_MS, now_ms)
            data = await _fetch_klines_batch(
                session, symbol, "1m", cursor, batch_end
            )
            if not data:
                break

            rows = []
            for k in data:
                rows.append((
                    int(k[0]),     # open_time_ms
                    float(k[1]),   # open
                    float(k[2]),   # high
                    float(k[3]),   # low
                    float(k[4]),   # close
                    float(k[5]),   # volume
                    float(k[7]),   # quote_volume
                    int(k[8]),     # trades_count
                    float(k[9]),   # taker_buy_volume
                    float(k[10]),  # taker_buy_quote_volume
                ))
            db.insert_rows(rows)
            total_inserted += len(rows)

            # Move cursor past the last candle we received
            last_time = int(data[-1][0])
            cursor = last_time + _1M_MS

            # Small delay to respect rate limits
            await asyncio.sleep(0.1)

    db.close()
    logger.info("%s: downloaded %d candles", symbol, total_inserted)
    return total_inserted


def ensure_klines_loading(db_root: str, symbol: str) -> Optional[asyncio.Task]:
    """
    Trigger background kline download if not already in progress.
    Returns the task, or None if already loaded/loading.
    """
    key = f"klines:{symbol}"
    if key in _loading:
        task = _loading[key]
        if not task.done():
            return task  # already in progress
        # Clean up finished task
        del _loading[key]

    # Check if DB already has recent data (within last 2 hours)
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data_manager"))
    from klines_db_manager import CandleDB

    db_path = Path(db_root) / symbol / f"{symbol}_1m.db"
    if db_path.exists():
        db = CandleDB(str(db_path))
        latest = db.get_latest_candle_time()
        db.close()
        if latest:
            age_ms = int(time.time() * 1000) - latest
            if age_ms < 2 * 60 * _1M_MS:  # less than 2 hours old
                return None  # data is fresh enough

    async def _download():
        try:
            await download_klines_for_symbol(db_root, symbol)
        except Exception as exc:
            logger.error("Background kline download failed for %s: %s", symbol, exc)
        finally:
            _loading.pop(key, None)

    task = asyncio.create_task(_download(), name=f"kline-load-{symbol}")
    _loading[key] = task
    return task
