from typing import List, Mapping, Sequence, Tuple

import decimal

import pytest

from binancebot import trader


class PlatonicMarket(trader.TradingClient):
    """An ideal market where all orders are immediately filled at the given price."""
    holdings: Mapping[trader.Asset, trader.Quantity]
    prices: Mapping[Tuple[trader.Asset, trader.Asset], trader.Price]
    commission: float
    orders: List[trader.OrderInfo]

    def __init__(self, holdings, prices, commission):
        self.holdings = holdings
        self.prices = prices
        self.commission = commission
        self.orders = []

    async def get_holdings(self) -> Mapping[trader.Asset, trader.Quantity]:
        return self.holdings.copy()

    async def get_price(self, base: trader.Asset, quote: trader.Asset) -> trader.Price:
        return self.prices[base, quote]

    async def get_order_info(self, order_id: int) -> trader.OrderInfo:
        return orders[order_id]

    async def buy_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.OrderInfo:
        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        price = self.prices[base, quote]

        if base_quantity:
            quote_quantity = base_quantity * price
        elif quote_quantity:
            base_quantity = quote_quantity / price

        executed_quantity = base_quantity * decimal.Decimal(1 - self.commission)
        cumulative_quote_quantity = quote_quantity

        assert self.holdings[quote] >= cumulative_quote_quantity

        self.holdings[base] = self.holdings.get(base, decimal.Decimal(0)) + executed_quantity
        self.holdings[quote] -= cumulative_quote_quantity
        if self.holdings[quote] == 0:
            del self.holdings[quote]

        status = trader.OrderInfo(
            len(self.orders),
            base=base,
            quote=quote,
            side=trader.OrderSide.BUY,
            original_quantity=base_quantity,
            executed_quantity=executed_quantity,
            cumulative_quote_quantity=cumulative_quote_quantity,
            status=trader.OrderStatus.FILLED,
        )

        print("BUY", executed_quantity, base, "for", cumulative_quote_quantity, quote)

        self.orders.append(status)
        return status

    async def sell_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.OrderInfo:
        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        price = self.prices[base, quote]

        if base_quantity:
            quote_quantity = base_quantity * price
        elif quote_quantity:
            base_quantity = quote_quantity / price

        executed_quantity = base_quantity
        cumulative_quote_quantity = quote_quantity * decimal.Decimal(1 - self.commission)

        assert self.holdings[base] >= executed_quantity

        self.holdings[base] -= executed_quantity
        if self.holdings[base] == 0:
            del self.holdings[base]
        self.holdings[quote] = self.holdings.get(quote, decimal.Decimal(0)) + cumulative_quote_quantity

        status = trader.OrderInfo(
            len(self.orders),
            base=base,
            quote=quote,
            side=trader.OrderSide.SELL,
            original_quantity=base_quantity,
            executed_quantity=executed_quantity,
            cumulative_quote_quantity=cumulative_quote_quantity,
            status=trader.OrderStatus.FILLED,
        )

        print("SELL", executed_quantity, base, "for", cumulative_quote_quantity, quote)

        self.orders.append(status)
        return status

    async def buy_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.OrderInfo:
        quote_quantity = base_quantity * price
        executed_quantity = base_quantity * decimal.Decimal(1 - self.commission)
        cumulative_quote_quantity = quote_quantity

        assert self.holdings[quote] >= cumulative_quote_quantity

        self.holdings[base] = self.holdings.get(base, decimal.Decimal(0)) + executed_quantity
        self.holdings[quote] -= cumulative_quote_quantity
        if self.holdings[quote] == 0:
            del self.holdings[quote]

        status = trader.OrderInfo(
            len(self.orders),
            base=base,
            quote=quote,
            side=trader.OrderSide.BUY,
            original_quantity=base_quantity,
            executed_quantity=executed_quantity,
            cumulative_quote_quantity=cumulative_quote_quantity,
            status=trader.OrderStatus.FILLED,
        )

        print("BUY", executed_quantity, base, "for", cumulative_quote_quantity, quote)

        self.orders.append(status)
        return status
        
    async def sell_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.OrderInfo:
        quote_quantity = base_quantity * price
        executed_quantity = base_quantity
        cumulative_quote_quantity = quote_quantity * decimal.Decimal(1 - self.commission)

        assert self.holdings[base] >= executed_quantity

        self.holdings[base] -= executed_quantity
        if self.holdings[base] == 0:
            del self.holdings[base]
        self.holdings[quote] = self.holdings.get(quote, decimal.Decimal(0)) + cumulative_quote_quantity

        status = trader.OrderInfo(
            len(self.orders),
            base=base,
            quote=quote,
            side=trader.OrderSide.SELL,
            original_quantity=base_quantity,
            executed_quantity=executed_quantity,
            cumulative_quote_quantity=cumulative_quote_quantity,
            status=trader.OrderStatus.FILLED,
        )

        print("SELL", executed_quantity, base, "for", cumulative_quote_quantity, quote)

        self.orders.append(status)
        return status


@pytest.mark.asyncio
async def test_reblance() -> None:
    market = PlatonicMarket(
        holdings={
            "BTC": decimal.Decimal("100.00000000"),
            "ETH": decimal.Decimal("100.00000000"),
        },
        prices={
            ("ETH", "BTC"): decimal.Decimal("1.00000000"),
            ("BNB", "BTC"): decimal.Decimal("0.50000000"),
            ("DOGE", "BTC"): decimal.Decimal("0.01000000"),
            ("BTC", "BUSD"): decimal.Decimal("10.00"),
            ("ETH", "BUSD"): decimal.Decimal("10.00"),
            ("BNB", "BUSD"): decimal.Decimal("5.00"),
            ("DOGE", "BUSD"): decimal.Decimal("0.10"),
            ("BTC", "BNB"): decimal.Decimal("2.00000000"),
        },
        commission=0.001,
    )

    target = {
        "BNB": 0.75,
        "ETH": 0.1,
        "DOGE": 0.15,
    }

    quote = "BTC"
    value = "BUSD"
    threshold = 1.05

    await trader.rebalance(target, value, quote, threshold, market)

    holdings = await market.get_holdings()
    values = {a: q * await market.get_price(a, value) for a, q in holdings.items()}
    total = sum(values.values())
    distribution = {k: float(v / total) for k, v in values.items()}

    assert all(d / target[k] < threshold for k, d in distribution.items())
    assert all(target[k] / d < threshold for k, d in distribution.items())
