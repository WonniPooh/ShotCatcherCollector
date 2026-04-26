# user_data_ws.py
"""
Binance Futures User Data WebSocket client.

Connects to the Binance User Data Stream, parses ORDER_TRADE_UPDATE and
ACCOUNT_UPDATE events, inserts them into SQLite, and broadcasts to UI
clients via WorkerManager._broadcast().

Lifecycle:
  1. Create listen key via REST
  2. Connect to wss://fstream.binance.com/private/ws?listenKey=<key>&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE
  3. Keepalive every 30 min
  4. Parse and dispatch events
  5. Reconnect on disconnect (with fresh listen key)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger("collector.user_data_ws")

MAINNET_WS = "wss://fstream.binance.com/private/ws?listenKey="

# Keepalive interval (Binance requires < 60min, we use 30min)
_KEEPALIVE_INTERVAL_S = 30 * 60

# If no WS message arrives within this window, force reconnect.
# Binance normally sends at least pong frames; a silent stream indicates
# the listen key may have been invalidated without a listenKeyExpired event.
_STALENESS_TIMEOUT_S = 5 * 60  # 5 minutes


class UserDataWS:
    """
    Async Binance Futures User Data Stream client.

    Usage:
        ws = UserDataWS(rest_client, on_order_event=..., on_account_update=...)
        ws.start()  # spawns background task
        ...
        await ws.stop()
    """

    def __init__(
        self,
        rest_client: Any,  # BinanceFuturesClient
        *,
        on_order_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_account_update: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_reconnect: Optional[Callable[[], None]] = None,
        reconnect_interval: float = 5.0,
    ):
        self._client = rest_client
        self._on_order_event = on_order_event
        self._on_account_update = on_account_update
        self._on_reconnect = on_reconnect
        self._base_ws = MAINNET_WS
        self._reconnect_interval = reconnect_interval

        self._task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._staleness_task: Optional[asyncio.Task] = None
        self._listen_key: Optional[str] = None
        self._connected = False
        self._ws: Any = None

        # Buffer for events received before sync completes
        self._buffer: List[Dict[str, Any]] = []
        self._buffering = True  # True until drain_buffer() is called

    # ── Public API ──────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def buffering(self) -> bool:
        return self._buffering

    def start(self) -> None:
        """Spawn the connection loop as a background task."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._connection_loop(),
            name="user-data-ws",
        )

    async def stop(self) -> None:
        """Cancel the connection loop and close cleanly."""
        for task_attr in ('_staleness_task', '_keepalive_task'):
            task = getattr(self, task_attr, None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, task_attr, None)

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        self._connected = False

    def drain_buffer(self) -> List[Dict[str, Any]]:
        """
        Return buffered events and switch to live mode.
        Call this after REST sync completes to get events that arrived during sync.
        """
        events = list(self._buffer)
        self._buffer.clear()
        self._buffering = False
        logger.info("user_data_ws: drained %d buffered events, switching to live mode", len(events))
        return events

    # ── Connection loop ─────────────────────────────────────────────────

    async def _connection_loop(self) -> None:
        """Reconnects forever until the task is cancelled."""
        reconnect_count = 0
        while True:
            try:
                # Get a fresh listen key
                reconnect_count += 1
                logger.info(
                    "user_data_ws: creating listen key (attempt #%d)...",
                    reconnect_count,
                )
                self._listen_key = await self._client.create_listen_key()
                if self._base_ws == MAINNET_WS:
                    ws_url = f"{self._base_ws}{self._listen_key}&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE"
                else:
                    ws_url = f"{self._base_ws}{self._listen_key}"
                logger.info("user_data_ws: connecting to %s...", ws_url[:60] + "...")

                async with websockets.connect(ws_url) as ws:
                    self._ws = ws
                    self._connected = True
                    self._last_event_time = time.time()
                    is_reconnect = reconnect_count > 1
                    logger.info(
                        "user_data_ws: connected (attempt #%d, reconnects=%d)",
                        reconnect_count, reconnect_count - 1,
                    )

                    # On reconnect, trigger account resync to fill the gap
                    if is_reconnect and self._on_reconnect:
                        logger.info("user_data_ws: reconnect detected — triggering account resync")
                        self._on_reconnect()

                    # Start keepalive loop
                    self._keepalive_task = asyncio.create_task(
                        self._keepalive_loop(),
                        name="user-data-keepalive",
                    )
                    # Start staleness watchdog
                    self._staleness_task = asyncio.create_task(
                        self._staleness_watchdog(ws),
                        name="user-data-staleness",
                    )

                    try:
                        async for raw in ws:
                            try:
                                msg = json.loads(raw)
                            except json.JSONDecodeError:
                                logger.warning("user_data_ws: non-JSON message: %s", raw[:200])
                                continue
                            if not isinstance(msg, dict):
                                logger.debug("user_data_ws: ignoring non-object message: %s", str(msg)[:200])
                                continue
                            self._last_event_time = time.time()
                            self._dispatch(msg)
                    finally:
                        for task_attr in ('_staleness_task', '_keepalive_task'):
                            task = getattr(self, task_attr, None)
                            if task is not None:
                                task.cancel()
                                try:
                                    await task
                                except asyncio.CancelledError:
                                    pass
                                setattr(self, task_attr, None)

            except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "user_data_ws: disconnected after %.1fs: %s",
                    time.time() - getattr(self, '_last_event_time', time.time()),
                    exc,
                )
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("user_data_ws: unexpected error: %s", exc, exc_info=True)
            finally:
                self._connected = False
                self._ws = None

            logger.info(
                "user_data_ws: reconnecting in %.1fs... (total reconnects=%d)",
                self._reconnect_interval, reconnect_count,
            )
            await asyncio.sleep(self._reconnect_interval)

    async def _keepalive_loop(self) -> None:
        """Send listen key keepalive every 30 minutes."""
        while True:
            await asyncio.sleep(_KEEPALIVE_INTERVAL_S)
            try:
                await self._client.keepalive_listen_key()
                logger.debug("user_data_ws: listen key keepalive sent")
            except Exception as exc:
                logger.warning("user_data_ws: keepalive failed: %s", exc)

    async def _staleness_watchdog(self, ws) -> None:
        """Close the WS if no messages arrive within _STALENESS_TIMEOUT_S.

        Binance normally sends data or at least pong frames frequently.
        A fully silent stream likely means the listen key was invalidated
        without a listenKeyExpired event, so we force a reconnect.
        """
        while True:
            await asyncio.sleep(60)  # check every minute
            elapsed = time.time() - self._last_event_time
            if elapsed >= _STALENESS_TIMEOUT_S:
                logger.warning(
                    "user_data_ws: no messages for %.0fs — forcing reconnect",
                    elapsed,
                )
                try:
                    await ws.close()
                except Exception:
                    pass
                return

    # ── Event dispatch ──────────────────────────────────────────────────

    def _dispatch(self, msg: Dict[str, Any]) -> None:
        """Route incoming WS messages to the appropriate handler."""
        event_type = msg.get("e")

        if event_type == "ORDER_TRADE_UPDATE":
            self._handle_order_trade_update(msg)
        elif event_type == "ACCOUNT_UPDATE":
            self._handle_account_update(msg)
        elif event_type == "listenKeyExpired":
            logger.warning("user_data_ws: listen key expired, will reconnect")
            # Close WS to trigger reconnect with fresh key
            if self._ws:
                asyncio.create_task(self._ws.close())
        elif event_type == "MARGIN_CALL":
            logger.warning("user_data_ws: MARGIN_CALL received: %s", msg)
        else:
            logger.debug("user_data_ws: unhandled event type: %s", event_type)

    def _handle_order_trade_update(self, msg: Dict[str, Any]) -> None:
        """
        Parse ORDER_TRADE_UPDATE and dispatch to callback.

        Binance format:
        {
          "e": "ORDER_TRADE_UPDATE",
          "E": 1700000000000,     // event time
          "T": 1700000000000,     // transaction time
          "o": {
            "s": "BTCUSDT",       // symbol
            "c": "client_id",     // clientOrderId
            "S": "BUY",           // side
            "o": "LIMIT",         // orderType
            "f": "GTC",           // timeInForce
            "q": "0.001",         // origQty
            "p": "50000",         // price
            "ap": "49999.5",      // avgPrice
            "sp": "0",            // stopPrice
            "x": "TRADE",         // executionType
            "X": "FILLED",        // orderStatus
            "i": 1000001,         // orderId
            "l": "0.001",         // lastFilledQty
            "z": "0.001",         // cumulativeFilledQty
            "L": "49999.5",       // lastFilledPrice
            "n": "0.02",          // commission
            "N": "USDT",          // commissionAsset
            "T": 1700000000000,   // orderTradeTime
            "t": 500001,          // tradeId
            "rp": "0",            // realizedProfit
            "m": false,           // isMaker
            "R": false,           // isReduceOnly
            "ps": "BOTH",         // positionSide
          }
        }
        """
        o = msg.get("o", {})
        event_time = msg.get("E", 0)

        parsed = {
            "s": o.get("s", ""),
            "c": o.get("c", ""),
            "S": o.get("S", "BUY"),
            "o": o.get("o", "LIMIT"),
            "f": o.get("f", "GTC"),
            "q": o.get("q", "0"),
            "p": o.get("p", "0"),
            "ap": o.get("ap", "0"),
            "sp": o.get("sp", "0"),
            "x": o.get("x", "NEW"),
            "X": o.get("X", "NEW"),
            "i": int(o.get("i", 0)),
            "l": o.get("l", "0"),
            "z": o.get("z", "0"),
            "L": o.get("L", "0"),
            "n": o.get("n", "0"),
            "N": o.get("N", ""),
            "T": int(o.get("T", 0)),
            "t": int(o.get("t", 0)),
            "rp": o.get("rp", "0"),
            "m": o.get("m", False),
            "R": o.get("R", False),
            "ps": o.get("ps", "BOTH"),
            "E": int(event_time),
        }

        if self._buffering:
            self._buffer.append(parsed)
            logger.debug(
                "user_data_ws: buffered %s for %s (order %d)",
                parsed["x"], parsed["s"], parsed["i"],
            )
        elif self._on_order_event:
            logger.info(
                "user_data_ws: dispatching %s %s for %s order=%d price=%s",
                parsed["x"], parsed["X"], parsed["s"], parsed["i"], parsed["p"],
            )
            self._on_order_event(parsed)

    def _handle_account_update(self, msg: Dict[str, Any]) -> None:
        """Parse ACCOUNT_UPDATE and dispatch to callback."""
        a = msg.get("a", {})
        parsed = {
            "event_time": int(msg.get("E", 0)),
            "transaction_time": int(msg.get("T", 0)),
            "event_reason": a.get("m", ""),
            "balances": a.get("B", []),
            "positions": a.get("P", []),
        }

        if self._on_account_update:
            self._on_account_update(parsed)

    # ── Test hooks ──────────────────────────────────────────────────────

    def inject_message_for_test(self, msg: Dict[str, Any]) -> None:
        """Test hook: inject a raw WS message for dispatch."""
        self._dispatch(msg)

    def set_buffering_for_test(self, buffering: bool) -> None:
        """Test hook: control buffering mode."""
        self._buffering = buffering
