# binance_futures_client.py
"""
Async Binance Futures REST client (signed requests).

Uses HMAC-SHA256 signing for USER_DATA endpoints.
Implements endpoints for account sync and order management:
  - GET  /fapi/v2/account
  - GET  /fapi/v1/allOrders
  - GET  /fapi/v1/userTrades
  - GET  /fapi/v1/orderAmendment
  - GET  /fapi/v1/openOrders
  - POST /fapi/v1/listenKey      (for User Data WS)
  - PUT  /fapi/v1/listenKey      (keepalive)
  - POST /fapi/v1/order          (place order)
  - DELETE /fapi/v1/order        (cancel order)
  - POST /fapi/v1/algoOrder       (place conditional/algo order)
  - DELETE /fapi/v1/algoOrder    (cancel conditional/algo order)
  - GET  /fapi/v1/ticker/price   (current price)
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger("collector.binance")

MAINNET_BASE = "https://fapi.binance.com"

class BinanceFuturesClient:
    """Async read-only Binance Futures REST client."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        recv_window: int = 5000,
        rate_limiter: Any = None,
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = MAINNET_BASE
        self._recv_window = recv_window
        self._rate_limiter = rate_limiter
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-MBX-APIKEY": self._api_key},
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ── signing ─────────────────────────────────────────────────────────

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Add timestamp and HMAC-SHA256 signature to params."""
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = self._recv_window
        query = urlencode(params)
        sig = hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = sig
        return params

    # ── raw request ─────────────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = True,
        weight: int = 1,
    ) -> Any:
        if params is None:
            params = {}

        # Acquire rate-limit capacity BEFORE signing (timestamp stays fresh)
        if self._rate_limiter:
            await self._rate_limiter.acquire(weight)

        if signed:
            params = self._sign(params)

        session = await self._get_session()
        url = f"{self._base_url}{path}"

        async with session.request(method, url, params=params) as resp:
            data = await resp.json()
            raw_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M")
            if raw_weight and self._rate_limiter:
                try:
                    self._rate_limiter.record_used_weight(int(raw_weight))
                except Exception:
                    pass
            if resp.status != 200:
                code = data.get("code", resp.status)
                msg = data.get("msg", str(data))
                raise BinanceAPIError(code, msg, path)

        # Backpressure check after successful response
        if self._rate_limiter:
            await self._rate_limiter.check_backpressure()

        return data

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = True, weight: int = 1) -> Any:
        return await self._request("GET", path, params, signed, weight)

    async def _post(self, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = True) -> Any:
        return await self._request("POST", path, params, signed)

    async def _put(self, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = True) -> Any:
        return await self._request("PUT", path, params, signed)

    async def _delete(self, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = True) -> Any:
        return await self._request("DELETE", path, params, signed)

    # ── account ─────────────────────────────────────────────────────────

    async def get_account(self) -> Dict[str, Any]:
        """GET /fapi/v2/account — weight 5."""
        return await self._get("/fapi/v2/account", weight=5)

    async def get_positions(self) -> List[Dict[str, Any]]:
        """Return positions with non-zero amount from account info."""
        account = await self.get_account()
        return [
            p for p in account.get("positions", [])
            if float(p.get("positionAmt", 0)) != 0.0
        ]

    # ── orders ──────────────────────────────────────────────────────────

    async def get_orders(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        order_id: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        GET /fapi/v1/allOrders — weight 5.
        Query period must be < 7 days. Max limit 1000.
        If orderId is set, returns orders with orderId >= that value.
        """
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if order_id is not None:
            params["orderId"] = order_id
        return await self._get("/fapi/v1/allOrders", params, weight=5)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """GET /fapi/v1/openOrders — weight 1 (with symbol) or 40."""
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return await self._get("/fapi/v1/openOrders", params, weight=1 if symbol else 40)

    # ── trades ──────────────────────────────────────────────────────────

    async def get_user_trades(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        from_id: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        GET /fapi/v1/userTrades — weight 5.
        Query period must be < 7 days. Max limit 1000.
        fromId cannot be combined with startTime/endTime.
        """
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        elif start_time is not None or end_time is not None:
            if start_time is not None:
                params["startTime"] = start_time
            if end_time is not None:
                params["endTime"] = end_time
        return await self._get("/fapi/v1/userTrades", params, weight=5)

    # ── order amendments ────────────────────────────────────────────────

    async def get_order_amendments(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        GET /fapi/v1/orderAmendment — weight 1.
        Default 50, max 100. Max 3 months of history.
        Either orderId or origClientOrderId required.
        """
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if order_id is not None:
            params["orderId"] = order_id
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return await self._get("/fapi/v1/orderAmendment", params)

    async def get_income_history(
        self,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """GET /fapi/v1/income — weight 30. Returns income records (PnL, commission, funding).

        No symbol param = all symbols. Max 1000 per page, paginate via startTime.
        """
        params: Dict[str, Any] = {"limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return await self._get("/fapi/v1/income", params, weight=30)

    # ── listen key ──────────────────────────────────────────────────────

    async def create_listen_key(self) -> str:
        """POST /fapi/v1/listenKey — weight 1. Returns listen key string."""
        data = await self._post("/fapi/v1/listenKey", signed=False)
        return data["listenKey"]

    async def keepalive_listen_key(self) -> None:
        """PUT /fapi/v1/listenKey — weight 1."""
        await self._put("/fapi/v1/listenKey", signed=False)

    # ── order management ────────────────────────────────────────────────

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[str] = None,
        price: Optional[str] = None,
        stop_price: Optional[str] = None,
        time_in_force: Optional[str] = None,
        reduce_only: Optional[bool] = None,
        new_client_order_id: Optional[str] = None,
        close_position: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        POST /fapi/v1/order — weight 0 (rate-limited by order rate).

        Supports: LIMIT, MARKET, STOP, STOP_MARKET, TAKE_PROFIT, TAKE_PROFIT_MARKET.
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
        }
        if quantity is not None:
            params["quantity"] = quantity
        if price is not None:
            params["price"] = price
        if stop_price is not None:
            params["stopPrice"] = stop_price
        if time_in_force is not None:
            params["timeInForce"] = time_in_force
        if reduce_only is not None:
            params["reduceOnly"] = str(reduce_only).lower()
        if new_client_order_id is not None:
            params["newClientOrderId"] = new_client_order_id
        if close_position is not None:
            params["closePosition"] = str(close_position).lower()
        return await self._post("/fapi/v1/order", params)

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        DELETE /fapi/v1/order — weight 1.
        Either orderId or origClientOrderId is required.
        """
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        return await self._delete("/fapi/v1/order", params)

    async def place_algo_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        trigger_price: str,
        price: Optional[str] = None,
        time_in_force: Optional[str] = None,
        working_type: str = "CONTRACT_PRICE",
        reduce_only: Optional[bool] = None,
        client_algo_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        POST /fapi/v1/algoOrder — conditional (STOP/STOP_MARKET/TAKE_PROFIT/etc).

        Returns dict with algoId, clientAlgoId, algoStatus, symbol, etc.
        """
        params: Dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity,
            "triggerPrice": trigger_price,
            "workingType": working_type,
        }
        if price is not None:
            params["price"] = price
        if time_in_force is not None:
            params["timeInForce"] = time_in_force
        if reduce_only is not None:
            params["reduceOnly"] = str(reduce_only).lower()
        if client_algo_id is not None:
            params["clientAlgoId"] = client_algo_id
        return await self._post("/fapi/v1/algoOrder", params)

    async def cancel_algo_order(
        self,
        algo_id: int,
    ) -> Dict[str, Any]:
        """
        DELETE /fapi/v1/algoOrder — cancel a conditional (algo) order.

        Uses algoId from place_algo_order response.
        """
        params: Dict[str, Any] = {
            "algoId": algo_id,
        }
        return await self._delete("/fapi/v1/algoOrder", params)

    async def get_ticker_price(self, symbol: str) -> Dict[str, Any]:
        """GET /fapi/v1/ticker/price — weight 1. Returns {symbol, price, time}."""
        return await self._get("/fapi/v1/ticker/price", {"symbol": symbol}, signed=False)

    # ── context manager ─────────────────────────────────────────────────

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()


class BinanceAPIError(Exception):
    """Raised when Binance returns a non-200 response."""

    def __init__(self, code: int, msg: str, path: str):
        self.code = code
        self.msg = msg
        self.path = path
        super().__init__(f"Binance API error {code} on {path}: {msg}")
