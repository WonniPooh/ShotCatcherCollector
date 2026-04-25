"""
WebSocket server for the Collector.

Listens on ws://localhost:<port>/ws for connections from the Chart UI Server.
Handles load requests, status queries, and streams back progress events.

Protocol: see docs/collector/architecture.md#ws-protocol
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Set

import websockets
from websockets.server import ServerConnection

logger = logging.getLogger("collector.ws_server")


class CollectorWSServer:
    def __init__(
        self,
        host: str,
        port: int,
        on_load_request: Callable[[str], None],
        on_cancel_request: Callable[[str], None],
        get_status: Callable[[str | None], dict],
        rest_client: Any = None,
        on_fill_gap: Callable[[str, int, int], None] | None = None,
    ):
        self._host = host
        self._port = port
        self._on_load = on_load_request
        self._on_cancel = on_cancel_request
        self._get_status = get_status
        self._rest_client = rest_client
        self._on_fill_gap = on_fill_gap

        self._clients: Set[ServerConnection] = set()
        self._server = None

    async def start(self) -> None:
        self._server = await websockets.serve(
            self._handle_client, self._host, self._port,
            ping_interval=None,  # localhost — no keepalive needed
        )
        logger.info("Collector WS server listening on ws://%s:%d/ws",
                    self._host, self._port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def broadcast(self, msg: dict) -> None:
        """Send a message to all connected clients (e.g. progress events)."""
        if not self._clients:
            return
        data = json.dumps(msg)
        # Fire and forget to all clients; drop on error
        results = await asyncio.gather(
            *[client.send(data) for client in self._clients],
            return_exceptions=True,
        )
        for client, result in zip(list(self._clients), results):
            if isinstance(result, Exception):
                logger.debug("Client send failed (likely disconnected): %s", result)
                self._clients.discard(client)

    async def send_to(self, client: ServerConnection, msg: dict) -> None:
        try:
            await client.send(json.dumps(msg))
        except Exception as exc:
            logger.debug("send_to failed: %s", exc)
            self._clients.discard(client)

    # ── Private ───────────────────────────────────────────────────────────────

    async def _handle_client(self, ws: ServerConnection) -> None:
        self._clients.add(ws)
        logger.info("Chart UI Server connected from %s", ws.remote_address)

        # Send current watchlist on connect
        status = self._get_status(None)
        await self.send_to(ws, {"type": "watching", "symbols": status.get("symbols", [])})

        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON from client: %r", raw)
                    continue
                await self._dispatch(ws, msg)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._clients.discard(ws)
            logger.info("Chart UI Server disconnected")

    async def _dispatch(self, ws: ServerConnection, msg: dict) -> None:
        t = msg.get("type")
        symbol = msg.get("symbol")

        if t == "load":
            if symbol:
                logger.info("← LOAD request from UI: symbol=%s", symbol)
                self._on_load(symbol)
            else:
                logger.warning("← LOAD request with no symbol: %r", msg)
        elif t == "cancel":
            if symbol:
                logger.info("← CANCEL request from UI: symbol=%s", symbol)
                self._on_cancel(symbol)
            else:
                logger.warning("← CANCEL request with no symbol: %r", msg)
        elif t == "fill_gap":
            gap_from = msg.get("from_ms")
            gap_to = msg.get("to_ms")
            if symbol and gap_from and gap_to:
                logger.info("← FILL_GAP request: symbol=%s  %d → %d", symbol, gap_from, gap_to)
                if self._on_fill_gap:
                    self._on_fill_gap(symbol, int(gap_from), int(gap_to))
            else:
                logger.warning("← FILL_GAP request missing fields: %r", msg)
        elif t == "status":
            logger.info("← STATUS query: symbol=%s", symbol)
            status = self._get_status(symbol)
            await self.send_to(ws, {"type": "status_response", **status})
        elif t == "list":
            logger.info("← LIST query")
            status = self._get_status(None)
            await self.send_to(ws, {"type": "list_response", **status})
        elif t == "get_open_orders":
            logger.info("← GET_OPEN_ORDERS request")
            await self._handle_get_open_orders(ws)
        else:
            logger.warning("← Unknown message type: %r  msg=%r", t, msg)

    async def _handle_get_open_orders(self, ws: ServerConnection) -> None:
        """Query Binance REST API for currently open orders and reply."""
        if not self._rest_client:
            await self.send_to(ws, {
                "type": "open_orders_response",
                "orders": [],
                "error": "no REST client",
            })
            return
        try:
            orders = await self._rest_client.get_open_orders()
            await self.send_to(ws, {
                "type": "open_orders_response",
                "orders": orders,
            })
            logger.info("→ Sent %d open orders from exchange", len(orders))
        except Exception as exc:
            logger.error("Failed to get open orders from exchange: %s", exc)
            await self.send_to(ws, {
                "type": "open_orders_response",
                "orders": [],
                "error": str(exc),
            })
