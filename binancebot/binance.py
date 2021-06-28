from typing import Any, Dict, List, Literal, Mapping, MutableMapping, Optional, Sequence, Tuple

import asyncio
import dataclasses
import enum
import hmac
import json
import logging
import time

import httpx

from binancebot import trader


logger = logging.getLogger(__name__)

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


class OrderStatus(enum.Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    PENDING_CANCEL = "PENDING_CANCEL"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"

    @property
    def is_open(self):
        return self in (OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED)


class Interval(enum.Enum):
    ONE_MINUTE = "1m"
    THREE_MINUTES = "3m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    TWO_HOURS = "2h"
    FOUR_HOURS = "4h"
    SIX_HOURS = "6h"
    EIGHT_HOURS = "8h"
    TWELVE_HOURS = "12h"
    ONE_DAY = "1d"
    THREE_DAYS = "3d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1M"

    @property
    def milliseconds(self) -> int:
        table = {
            "m": 60 * 1000,
            "h": 60 * 60 * 1000,
            "d": 24 * 60 * 60 * 1000,
            "w": 7 * 24 * 60 * 60 * 1000,
            "M": 30 * 7 * 24 * 60 * 60 * 1000,
        }
        return int(self.value[:-1]) * table[self.value[-1]]


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


@dataclasses.dataclass(frozen=True)
class LotSizeFilter:
    min: Optional[trader.Quantity]
    max: Optional[trader.Quantity]
    step: Optional[trader.Quantity]


@dataclasses.dataclass(frozen=True)
class PriceFilter:
    min: Optional[trader.Price]
    max: Optional[trader.Price]
    step: Optional[trader.Price]


@dataclasses.dataclass(frozen=True)
class PercentPriceFilter:
    multiplier_up: trader.Price
    multiplier_down: trader.Price
    window_minutes: int


@dataclasses.dataclass(frozen=True)
class LimitNotionalValueFilter:
    min: trader.Quantity


@dataclasses.dataclass(frozen=True)
class MarketNotionalValueFilter:
    min: trader.Quantity
    window_minutes: int


@dataclasses.dataclass(frozen=True)
class SymbolInfo:
    base: trader.Asset
    quote: trader.Asset
    base_precision: int
    quote_precision: int
    max_orders: Optional[int]
    """Number of orders that can be open concurrently at any time."""
    max_position: Optional[trader.Quantity]
    """Maximum quantity of base asset that can be held by an account.
    Orders that if filled would result in a position larger than this are rejected."""
    price_filter: Optional[PriceFilter]
    percent_price_filter: Optional[PercentPriceFilter]
    limit_lot_size_filter: Optional[LotSizeFilter]
    limit_notional_value_filter: Optional[LimitNotionalValueFilter]
    market_lot_size_filter: Optional[LotSizeFilter]
    market_notional_value_filter: Optional[MarketNotionalValueFilter]


class BinanceAPIError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return f"{self.message} (code {self.code})"


class BinanceClient(trader.TradingClient):
    api_base: str
    api_key: str
    secret_key: bytes
    http_client: httpx.AsyncClient
    symbol_info: Dict[Symbol, SymbolInfo]
    symbols: Dict[Tuple[trader.Asset, trader.Asset], Symbol]

    def __init__(self, api_base, api_key, secret_key, http_client):
        self.api_base = api_base
        self.api_key = api_key
        self.secret_key = secret_key
        self.http_client = http_client
        self.symbol_info = {}
        self.symbols = {}
        self.symbols_last_fetched = 0
        self.symbols_lock = asyncio.Lock()

    # --------------------
    # TraderClient API
    # --------------------

    async def get_holdings(self) -> Mapping[trader.Asset, trader.Quantity]:
        response_json = await self.binance_rest("/api/v3/account", signed=True)
        logger.debug(response_json)
        return {
            balance["asset"]: trader.Quantity(balance["free"]) + trader.Quantity(balance["locked"])
            for balance in response_json["balances"]
            if trader.Quantity(balance["free"]) + trader.Quantity(balance["locked"]) > 0
        }

    async def get_commission(self) -> float:
        response_json = await self.binance_rest("/api/v3/account", signed=True)
        return response_json["takerCommission"] / 10000

    async def get_price(self, base: trader.Asset, quote: trader.Asset) -> trader.Price:
        symbol = await self.get_symbol(base, quote)
        response_json = await self.binance_rest("/api/v3/avgPrice", params={"symbol": symbol})
        return trader.Price(response_json['price'])

    async def apply_market_filters(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> List[trader.MarketOrder]:
        """Returns the closest trade(s) that satisfies the filters."""
        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        market_price = await self.get_price(base, quote)

        if quote_quantity:
            quantity = quote_quantity / market_price
        elif base_quantity:
            quantity = base_quantity

        symbol = await self.get_symbol(base, quote)
        symbol_info = await self.get_symbol_info(symbol)

        if symbol_info.market_notional_value_filter is not None:
            g = symbol_info.market_notional_value_filter
            assert g is not None

            logger.debug(f"notional: {base} {quote}")
            logger.debug(f"notional: quantity in {quantity}")

            # ignore window_minutes and just use get_price
            if quantity * market_price < g.min:
                quantity = g.min / market_price

            logger.debug(f"notional: price {market_price}, min {g.min}")
            logger.debug(f"notional: quantity out {quantity}")

        if symbol_info.market_lot_size_filter is None:
            splits = [quantity]
        else:
            f = symbol_info.market_lot_size_filter
            assert f is not None

            def ceildiv(a, b):
                return a // b + (1 if a % b > 0 else 0)

            if f.step and f.min:
                quantity = ceildiv((quantity - f.min), f.step) * f.step + f.min
            elif f.step:
                quantity = ceildiv(quantity, f.step) * f.step

            if f.min and quantity < f.min:
                splits = [f.min]
            elif f.max and quantity > f.max:
                # break into smaller, valid orders
                if f.step:
                    chunk_size = ceildiv(f.max / 2, f.step) * f.step
                else:
                    chunk_size = f.max / 2

                assert not f.min or f.min < chunk_size

                splits = [chunk_size] * int(quantity // chunk_size)
                splits[-1] += quantity % chunk_size

                assert not f.step or all(split % f.step == 0 for split in splits)
            else:
                splits = [quantity]

        if quote_quantity:
            return [
                trader.MarketOrder(
                    base=base,
                    quote=quote,
                    quote_quantity=round(
                        split * market_price,
                        symbol_info.quote_precision,
                    ),
                )
                for split in splits
            ]
        else:
            return [
                trader.MarketOrder(
                    base=base,
                    quote=quote,
                    base_quantity=round(split, symbol_info.base_precision),
                )
                for split in splits
            ]

    async def apply_limit_filters(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        price: trader.Price,
        base_quantity: trader.Quantity,
    ) -> List[trader.LimitOrder]:
        """Returns the closest trade(s) that satisfies the filters."""
        symbol = await self.get_symbol(base, quote)
        symbol_info = await self.get_symbol_info(symbol)

        base_quantity = round(base_quantity, symbol_info.base_precision)

        if symbol_info.price_filter is not None:
            f = symbol_info.price_filter
            assert f is not None
            if f.min and price < f.min:
                price = f.min
            elif f.max and price > f.max:
                price = f.max
            elif f.min and f.step:
                price = ((price - f.min) // f.step) * f.step + f.min
            elif f.step:
                price = (price // f.step) * f.step

        if symbol_info.limit_notional_value_filter is not None:
            g = symbol_info.limit_notional_value_filter
            assert g is not None

            if base_quantity * price < g.min:
                base_quantity = g.min / price

        if symbol_info.limit_lot_size_filter is None:
            splits = [base_quantity]
        else:
            quantity = base_quantity
            f_ = symbol_info.market_lot_size_filter
            assert f_ is not None

            if f_.step and f_.min:
                quantity = ((quantity - f_.min) // f_.step) * f_.step + f_.min
            elif f_.step:
                quantity = (quantity // f_.step) * f_.step

            if f_.min and quantity < f_.min:
                splits = [f_.min]
            elif f_.max and quantity > f_.max:
                # break into smaller, valid orders
                if f_.step:
                    chunk_size = ((f_.max / 2) // f_.step) * f_.step
                else:
                    chunk_size = f_.max / 2

                assert not f_.min or f_.min < chunk_size

                splits = [chunk_size] * int(quantity // chunk_size)
                splits[-1] += quantity % chunk_size

                assert not f_.step or all(split % f_.step == 0 for split in splits)
            else:
                splits = [quantity]

        return [
            trader.LimitOrder(
                base=base,
                quote=quote,
                price=price,
                base_quantity=round(split, symbol_info.base_precision),
            )
            for split in splits
        ]

    async def buy_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.ExecutedOrder:
        return await self.submit_market_order(
            side=OrderSide.BUY,
            base=base,
            quote=quote,
            base_quantity=base_quantity,
            quote_quantity=quote_quantity,
        )

    async def sell_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.ExecutedOrder:
        return await self.submit_market_order(
            side=OrderSide.SELL,
            base=base,
            quote=quote,
            base_quantity=base_quantity,
            quote_quantity=quote_quantity,
        )

    async def buy_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.ExecutedOrder:
        return await self.submit_limit_order(
            side=OrderSide.BUY,
            base=base,
            quote=quote,
            price=price,
            base_quantity=base_quantity,
            time_in_force=TimeInForce.IMMEDIATE_OR_CANCEL,
        )

    async def sell_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.ExecutedOrder:
        return await self.submit_limit_order(
            side=OrderSide.SELL,
            base=base,
            quote=quote,
            price=price,
            base_quantity=base_quantity,
            time_in_force=TimeInForce.IMMEDIATE_OR_CANCEL,
        )

    async def submit_limit_order(
        self,
        side: OrderSide,
        base: trader.Asset,
        quote: trader.Asset,
        time_in_force: TimeInForce,
        price: trader.Price,
        base_quantity: trader.Quantity,
    ) -> trader.ExecutedOrder:
        symbol = await self.get_symbol(base, quote)
        params = dict(
            symbol=symbol,
            side=side.value,
            type=OrderType.LIMIT.value,
            timeInForce=time_in_force.value,
            price=price,
            quantity=base_quantity,
            newOrderRespType="RESULT",
        )
        logger.debug(f"submitting order {params}")
        response_json = await self.binance_rest(
            "/api/v3/order",
            params=params,
            signed=True,
            method="post",
        )
        return await self.chase_order(base, quote, response_json)

    async def submit_market_order(
        self,
        side: OrderSide,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.ExecutedOrder:
        symbol = await self.get_symbol(base, quote)

        params: Dict[str, Any]
        params = dict(
            symbol=symbol,
            side=side.value,
            type=OrderType.MARKET.value,
            newOrderRespType="FULL",
        )

        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        if base_quantity:
            params["quantity"] = base_quantity
        elif quote_quantity:
            params["quoteOrderQty"] = quote_quantity

        logger.debug(f"submitting order {params}")

        response_json = await self.binance_rest(
            "/api/v3/order", params=params, signed=True, method="post",
        )
        return await self.chase_order(base, quote, response_json)

    async def chase_order(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        response_json: Dict[str, Any],
    ) -> trader.ExecutedOrder:
        if OrderStatus(response_json["status"]).is_open:
            await asyncio.sleep(0.5)
            info = await self.get_order_info(response_json["symbol"], response_json["orderId"])
            while OrderStatus(info["status"]).is_open:
                await asyncio.sleep(0.5)
                info = await self.get_order_info(response_json["symbol"], response_json["orderId"])
            return trader.ExecutedOrder(
                base=base,
                quote=quote,
                side=trader.OrderSide(info["side"]),
                fills=[
                    trader.Fill(
                        price=trader.Price(info["price"]),
                        base_quantity=trader.Quantity(info["executedQty"]),
                        quote_quantity=trader.Quantity(info["cummulativeQuoteQty"]),
                    ),
                ],
            )
        else:
            return trader.ExecutedOrder(
                base=base,
                quote=quote,
                side=trader.OrderSide(response_json["side"]),
                fills=[
                    trader.Fill(
                        price=trader.Price(response_json["price"]),
                        base_quantity=trader.Quantity(response_json["executedQty"]),
                        quote_quantity=trader.Quantity(response_json["cummulativeQuoteQty"]),
                    ),
                ],
            )

    async def get_order_info(self, symbol: Symbol, order_id: int) -> Dict[str, Any]:
        response_json = await self.binance_rest(
            "/api/v3/order",
            params=dict(symbol=symbol, orderId=order_id),
            signed=True,
        )
        return response_json

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

    async def get_candlesticks(
        self, base: trader.Asset, quote: trader.Asset, period_ms: int, interval: Interval = Interval.ONE_MINUTE,
    ) -> List[Candlestick]:
        """Get recent aggregate price data for an asset pair."""
        assert period_ms <= 1000 * interval.milliseconds, (period_ms, interval)

        symbol = await self.get_symbol(base, quote)

        endTime = now()
        startTime = endTime - period_ms

        response_json = await self.binance_rest(
            "/api/v3/klines",
            params=dict(
                symbol=symbol,
                interval=interval.value,
                startTime=startTime,
                endTime=endTime,
                limit=1000
            ),
        )

        candlesticks = [
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
        candlesticks.sort(key=lambda c: c.open_time)
        return candlesticks

    async def get_vwap(
        self, base: trader.Asset, quote: trader.Asset, period_ms: int, interval: Interval
    ) -> trader.Price:
        """Find the volume weighted average price using candlestick data."""
        candlesticks = await self.get_candlesticks(base, quote, period_ms, interval)
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
        await self.get_symbols()
        return self.symbols[base, quote]

    async def get_symbol_info(self, symbol: Symbol) -> SymbolInfo:
        """Get the symbol for a base and quote asset pair."""
        await self.get_symbols()
        return self.symbol_info[symbol]

    async def get_symbols(self) -> Mapping[Tuple[trader.Asset, trader.Asset], Symbol]:
        """Fetch the list of active symbols."""
        async with self.symbols_lock:
            if not self.symbols or now() - self.symbols_last_fetched > 3_600_000:
                response_json = await self.binance_rest("/api/v3/exchangeInfo")

                logger.info(f"exchange time: {response_json['serverTime']} bot time: {(now_:=now())} diff: {now_ - response_json['serverTime']}")

                self.symbol_info = {}
                self.symbols = {}

                for s in response_json["symbols"]:
                    max_orders = None
                    max_position = None
                    price_filter = None
                    percent_price_filter = None
                    limit_lot_size_filter = None
                    limit_notional_value_filter = None
                    market_lot_size_filter = None
                    market_notional_value_filter = None

                    min_: Optional[trader.Quantity]
                    max_: Optional[trader.Quantity]
                    step: Optional[trader.Quantity]

                    for f in s["filters"]:
                        if f["filterType"] == "PRICE_FILTER":
                            min_p = trader.Price(f["minPrice"])
                            max_p = trader.Price(f["maxPrice"])
                            step_p = trader.Price(f["tickSize"])
                            price_filter = PriceFilter(
                                min=min_p if min_p else None,
                                max=max_p if max_p else None,
                                step=step_p if step_p else None,
                            )
                        elif f["filterType"] == "PERCENT_PRICE":
                            percent_price_filter = PercentPriceFilter(
                                multiplier_up=trader.Price(f["multiplierUp"]),
                                multiplier_down=trader.Price(f["multiplierDown"]),
                                window_minutes=f["avgPriceMins"],
                            )
                        elif f["filterType"] == "LOT_SIZE":
                            min_ = trader.Quantity(f["minQty"])
                            max_ = trader.Quantity(f["maxQty"])
                            step = trader.Quantity(f["stepSize"])
                            limit_lot_size_filter = LotSizeFilter(
                                min=min_ if min_ else None,
                                max=max_ if max_ else None,
                                step=step if step else None,
                            )
                        elif f["filterType"] == "MIN_NOTIONAL":
                            limit_notional_value_filter = LimitNotionalValueFilter(
                                min=trader.Quantity(f["minNotional"]),
                            )
                            if f["applyToMarket"]:
                                market_notional_value_filter = MarketNotionalValueFilter(
                                    min=trader.Quantity(f["minNotional"]),
                                    window_minutes=f["avgPriceMins"],
                                )
                        elif f["filterType"] == "MARKET_LOT_SIZE":
                            market_lot_size_filter = LotSizeFilter(
                                min=trader.Quantity(f["minQty"]),
                                max=trader.Quantity(f["maxQty"]),
                                step=trader.Quantity(f["stepSize"]),
                            )
                        elif f["filterType"] == "MAX_NUM_ORDERS":
                            max_orders = f["maxNumOrders"]
                        elif f["filterType"] == "MAX_POSITION":
                            max_position = trader.Quantity(f["maxPosition"])

                    if limit_lot_size_filter and market_lot_size_filter:
                        a = limit_lot_size_filter
                        b = market_lot_size_filter
                        assert a is not None and b is not None

                        if a.min and b.min:
                            min_ = max(a.min, b.min)
                        elif a.min:
                            min_ = a.min
                        else:
                            min_ = None

                        if a.max and b.max:
                            max_ = min(a.max, b.max)
                        elif a.max:
                            max_ = a.max
                        else:
                            max_ = None

                        if a.step and b.step:
                            step = max(a.step, b.step)
                            assert step % a.step == 0 and step % b.step == 0
                            step = step
                        elif a.step:
                            step = a.step
                        else:
                            step = None

                        market_lot_size_filter = LotSizeFilter(min_, max_, step)

                    self.symbol_info[s["symbol"]] = SymbolInfo(
                        base=s["baseAsset"],
                        quote=s["quoteAsset"],
                        base_precision=s["baseAssetPrecision"],
                        quote_precision=s["quoteAssetPrecision"],
                        max_orders=max_orders,
                        max_position=max_position,
                        price_filter=price_filter,
                        percent_price_filter=percent_price_filter,
                        limit_lot_size_filter=limit_lot_size_filter,
                        limit_notional_value_filter=limit_notional_value_filter,
                        market_lot_size_filter=market_lot_size_filter,
                        market_notional_value_filter=market_notional_value_filter,
                    )

                    self.symbols[s["baseAsset"], s["quoteAsset"]] = s["symbol"]

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

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                response_json = response.json()
            except json.decoder.JSONDecodeError:
                pass
            else:
                raise BinanceAPIError(response_json["code"], response_json["msg"]) from e
            raise e

        response_json = response.json()
        return response_json
