from typing import Any, Dict, List, Literal, Mapping, MutableMapping, Sequence, Tuple

import asyncio
import enum
import dataclasses
import hmac
import time

import httpx

from binancebot import trader


Symbol = str
Timestamp = int  # time in ms since epoch


def now() -> Timestamp:
    return time.time_ns() // 1_000_000


class TimeInForce(enum.Enum):
    GOOD_UNTIL_CANCELED = "GTC"
    IMMEDIATE_OR_CANCEL = "IOC"
    FILL_OR_KILL = "FOK"


class OrderSide(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(enum.Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_LOSS = "STOP_LOSS"
    STOP_LOSS_LIMIT = "STOP_LOSS_LIMIT"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"
    LIMIT_MAKER = "LIMIT_MAKER"


@dataclasses.dataclass
class Candlestick:
    open_time: Timestamp
    close_time: Timestamp
    open: trader.Price
    high: trader.Price
    low: trader.Price
    close: trader.Price
    volume: trader.Quantity


@dataclasses.dataclass(frozen=True)
class BidOrder:
    price: trader.Price
    quantity: trader.Quantity


@dataclasses.dataclass(frozen=True)
class AskOrder:
    price: trader.Price
    quantity: trader.Quantity


@dataclasses.dataclass(frozen=True)
class OrderBook:
    bids: Sequence[BidOrder]
    asks: Sequence[AskOrder]


class BinanceClient(trader.TradingClient):
    def __init__(self, api_base, api_key, secret_key, http_client):
        self.api_base = api_base
        self.api_key = api_key
        self.secret_key = secret_key
        self.http_client = http_client
        self.symbols = {}
        self.symbols_last_fetched = 0

    # --------------------
    # TraderClient API
    # --------------------

    async def get_holdings(self) -> Mapping[trader.Asset, trader.Quantity]:
        response_json = await self.binance_rest("/api/v3/account", signed=True)
        return {
            balance["asset"]: trader.Quantity(balance["free"])
            for balance in response_json["balances"]
        }

    async def get_price(self, base: trader.Asset, quote: trader.Asset) -> trader.Price:
        symbol = await self.get_symbol(base, quote)
        response_json = await self.binance_rest("/api/v3/avgPrice", params={"symbol": symbol})
        return trader.Price(response_json['price'])

    async def get_order_info(self, order_id: Tuple[Symbol, int]) -> trader.OrderInfo:
        symbol, id_ = order_id
        symbols, response_json = await asyncio.gather(
            self.get_symbols(),
            self.binance_rest(
                "/api/v3/order",
                params=dict(symbol=symbol, orderId=id_),
                signed=True,
            ),
        )
        # TODO: this is inefficent, maybe add a reverse index
        base, quote = next(iter(filter(lambda k: symbols[k] == symbol, symbols)))
        return trader.OrderInfo(
            order_id=order_id,
            base=base,
            quote=quote,
            side=trader.OrderSide(response_json["side"]),
            original_quantity=trader.Quantity(response_json["origQty"]),
            executed_quantity=trader.Quantity(response_json["executedQty"]),
            cumulative_quote_quantity=trader.Quantity(response_json["cummulativeQuoteQty"]),
            status=trader.OrderStatus(response_json["status"]),
        )

    async def buy_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.OrderInfo:
        symbol = await self.get_symbol(base, quote)
        params: Dict[str, Any]
        params = dict(
            symbol=symbol,
            side=OrderSide.BUY.value,
            type=OrderType.MARKET.value,
            newOrderRespType="FULL",
        )

        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        if base_quantity:
            params["quantity"] = base_quantity
        elif quote_quantity:
            params["quoteOrderQty"] = quote_quantity

        response_json = await self.binance_rest(
            "/api/v3/order", params=params, signed=True, method="post",
        )
        return trader.OrderInfo(
            order_id=(symbol, response_json["orderId"]),
            base=base,
            quote=quote,
            side=trader.OrderSide(response_json["side"]),
            original_quantity=trader.Quantity(response_json["origQty"]),
            executed_quantity=trader.Quantity(response_json["executedQty"]),
            cumulative_quote_quantity=trader.Quantity(response_json["cummulativeQuoteQty"]),
            status=trader.OrderStatus(response_json["status"]),
        )

    async def sell_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.OrderInfo:
        symbol = await self.get_symbol(base, quote)
        params: Dict[str, Any]
        params = dict(
            symbol=symbol,
            side=OrderSide.SELL.value,
            type=OrderType.MARKET.value,
            newOrderRespType="FULL",
        )

        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        if base_quantity:
            params["quantity"] = base_quantity
        elif quote_quantity:
            params["quoteOrderQty"] = quote_quantity

        response_json = await self.binance_rest(
            "/api/v3/order",
            params=params,
            signed=True,
            method="post",
        )
        return trader.OrderInfo(
            order_id=(symbol, response_json["orderId"]),
            base=base,
            quote=quote,
            side=trader.OrderSide(response_json["side"]),
            original_quantity=trader.Quantity(response_json["origQty"]),
            executed_quantity=trader.Quantity(response_json["executedQty"]),
            cumulative_quote_quantity=trader.Quantity(response_json["cummulativeQuoteQty"]),
            status=trader.OrderStatus(response_json["status"]),
        )

    async def buy_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.OrderInfo:
        symbol = await self.get_symbol(base, quote)
        response_json = await self.binance_rest(
            "/api/v3/order",
            params=dict(
                symbol=symbol,
                side=OrderSide.BUY.value,
                type=OrderType.LIMIT.value,
                timeInForce=TimeInForce.IMMEDIATE_OR_CANCEL.value,
                price=price,
                quantity=base_quantity,
                newOrderRespType="RESULT",
            ),
            signed=True,
            method="post",
        )
        return trader.OrderInfo(
            order_id=(symbol, response_json["orderId"]),
            base=base,
            quote=quote,
            side=trader.OrderSide(response_json["side"]),
            original_quantity=trader.Quantity(response_json["origQty"]),
            executed_quantity=trader.Quantity(response_json["executedQty"]),
            cumulative_quote_quantity=trader.Quantity(response_json["cummulativeQuoteQty"]),
            status=trader.OrderStatus(response_json["status"]),
        )

    async def sell_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.OrderInfo:
        symbol = await self.get_symbol(base, quote)
        response_json = await self.binance_rest(
            "/api/v3/order",
            params=dict(
                symbol=symbol,
                side=OrderSide.SELL.value,
                type=OrderType.LIMIT.value,
                timeInForce=TimeInForce.IMMEDIATE_OR_CANCEL.value,
                price=price,
                quantity=base_quantity,
                newOrderRespType="RESULT",
            ),
            signed=True,
            method="post",
        )
        return trader.OrderInfo(
            order_id=(symbol, response_json["orderId"]),
            base=base,
            quote=quote,
            side=trader.OrderSide(response_json["side"]),
            original_quantity=trader.Quantity(response_json["origQty"]),
            executed_quantity=trader.Quantity(response_json["executedQty"]),
            cumulative_quote_quantity=trader.Quantity(response_json["cummulativeQuoteQty"]),
            status=trader.OrderStatus(response_json["status"]),
        )

    # --------------------
    # Additional info
    # --------------------

    async def get_order_book(self, base: trader.Asset, quote: trader.Asset, client: httpx.AsyncClient) -> OrderBook:
        symbol = await self.get_symbol(base, quote)
        response_json = await self.binance_rest("/api/v3/depth", params=dict(symbol=symbol, limit=100))
        return OrderBook(
            bids=[BidOrder(a, b) for a, b in response_json["bids"]],
            asks=[AskOrder(a, b) for a, b in response_json["asks"]],
        )

    async def get_candlesticks(self, base: trader.Asset, quote: trader.Asset, period_ms: int) -> List[Candlestick]:
        """Get recent aggregate price data for an asset pair."""
        symbol = await self.get_symbol(base, quote)

        endTime = now()
        startTime = endTime - period_ms

        response_json = await self.binance_rest(
            "/api/v3/klines",
            params=dict(
                symbol=symbol,
                interval="1m",
                startTime=startTime,
                endTime=endTime,
                limit=1000
            ),
        )

        return [
            Candlestick(
                open_time=c[0],
                open=trader.Price(c[1]),
                high=trader.Price(c[2]),
                low=trader.Price(c[3]),
                close=trader.Price(c[4]),
                volume=trader.Quantity(c[5]),
                close_time=c[6],
            )
            for c in response_json
        ]

    async def get_vwap(self, base: trader.Asset, quote: trader.Asset, period_ms: int) -> trader.Price:
        """Find the volume weighted average price using candlestick data."""
        candlesticks = await self.get_candlesticks(base, quote, period_ms)
        assert candlesticks

        # numerically stable rolling weighted mean
        vwap = (candlesticks[-1].close + candlesticks[-1].high + candlesticks[-1].low) / 3
        volume = candlesticks[-1].volume
        for c in candlesticks[-2::-1]:
            volume += c.volume
            if volume > 0:
                vwap += ((c.close + c.high + c.low) / 3 - vwap) * c.volume / volume

        return vwap

    # --------------------
    # Utility methods
    # --------------------

    async def get_symbol(self, base: trader.Asset, quote: trader.Asset) -> Symbol:
        """Get the symbol for a base and quote asset pair."""
        symbols = await self.get_symbols()
        return symbols[base, quote]

    async def get_symbols(self) -> Mapping[Tuple[trader.Asset, trader.Asset], Symbol]:
        """Fetch the list of active symbols."""
        if not self.symbols or now() - self.symbols_last_fetched > 3_600_000:
            response_json = await self.binance_rest("/api/v3/exchangeInfo")
            self.symbols = {
                (symbol["baseAsset"], symbol["quoteAsset"]): symbol["symbol"]
                for symbol in response_json["symbols"]
            }
            self.symbols_last_fetched = now()
        return self.symbols

    async def binance_rest(
        self,
        endpoint: str,
        *,
        params: MutableMapping[str, Any] = None,
        headers: MutableMapping[str, Any] = None,
        signed: bool = False,
        method: Literal["get", "post"] = "get",
    ) -> Any:
        """Makes a request against the binance REST api."""
        if params is None:
            params = {}

        if headers is None:
            headers = {}

        if signed:
            # timestamp is in ms since 1970-01-01 UTC
            params["timestamp"] = now()

            totalParams = "&".join("{}={}".format(k, v) for k, v in params.items()).encode("utf-8")
            params["signature"] = hmac.new(self.secret_key, totalParams, "sha256").hexdigest()

            headers["X-MBX-APIKEY"] = self.api_key

        response = await getattr(self.http_client, method)(self.api_base + endpoint, params=params, headers=headers)
        response_json = response.json()

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"{method.upper()} {self.api_base + endpoint}?{'&'.join('{}={}'.format(k, v) for k, v in params.items())}: "
                f"HTTP {response.status_code} / Binance {response_json['code']}: {response_json['msg']}"
            ) from e

        return response_json
