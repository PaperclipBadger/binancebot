from typing import Dict, List, Mapping, Tuple

import decimal

import pytest

from binancebot import trader


class PlatonicMarket(trader.TradingClient):
    """An ideal market where all orders are immediately filled at the given price."""
    holdings: Dict[trader.Asset, trader.Quantity]
    prices: Dict[Tuple[trader.Asset, trader.Asset], trader.Price]
    commission: float

    def __init__(self, holdings, prices, commission):
        self.holdings = holdings
        self.prices = prices
        self.commission = commission

    async def get_holdings(self) -> Mapping[trader.Asset, trader.Quantity]:
        return self.holdings.copy()

    async def get_price(self, base: trader.Asset, quote: trader.Asset) -> trader.Price:
        return self.prices[base, quote]

    async def apply_market_filters(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> List[trader.MarketOrder]:
        return [trader.MarketOrder(base, quote, base_quantity, quote_quantity)]

    async def apply_limit_filters(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        price: trader.Price,
        base_quantity: trader.Quantity,
    ) -> List[trader.LimitOrder]:
        return [trader.LimitOrder(base, quote, price, base_quantity)]

    async def buy_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.ExecutedOrder:
        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        price = self.prices[base, quote]

        if base_quantity:
            quote_quantity = base_quantity * price
        elif quote_quantity:
            base_quantity = quote_quantity / price

        assert base_quantity is not None
        assert quote_quantity is not None

        executed_quantity = base_quantity * decimal.Decimal(1 - self.commission)
        cumulative_quote_quantity = quote_quantity

        assert self.holdings[quote] >= cumulative_quote_quantity

        self.holdings[base] = self.holdings.get(base, decimal.Decimal(0)) + executed_quantity
        self.holdings[quote] -= cumulative_quote_quantity
        if self.holdings[quote] == 0:
            del self.holdings[quote]

        status = trader.ExecutedOrder(
            base=base,
            quote=quote,
            side=trader.OrderSide.SELL,
            fills=[
                trader.Fill(
                    price=price,
                    base_quantity=executed_quantity,
                    quote_quantity=cumulative_quote_quantity,
                )
            ],
        )

        print("BUY", executed_quantity, base, "for", cumulative_quote_quantity, quote)

        return status

    async def sell_at_market(
        self,
        base: trader.Asset,
        quote: trader.Asset,
        *,
        base_quantity: trader.Quantity = None,
        quote_quantity: trader.Quantity = None,
    ) -> trader.ExecutedOrder:
        assert base_quantity or quote_quantity
        assert not (base_quantity and quote_quantity)

        price = self.prices[base, quote]

        if base_quantity:
            quote_quantity = base_quantity * price
        elif quote_quantity:
            base_quantity = quote_quantity / price

        assert base_quantity is not None
        assert quote_quantity is not None

        executed_quantity = base_quantity
        cumulative_quote_quantity = quote_quantity * decimal.Decimal(1 - self.commission)

        assert self.holdings[base] >= executed_quantity

        self.holdings[base] -= executed_quantity
        if self.holdings[base] == 0:
            del self.holdings[base]
        self.holdings[quote] = self.holdings.get(quote, decimal.Decimal(0)) + cumulative_quote_quantity

        status = trader.ExecutedOrder(
            base=base,
            quote=quote,
            side=trader.OrderSide.SELL,
            fills=[
                trader.Fill(
                    price=price,
                    base_quantity=executed_quantity,
                    quote_quantity=cumulative_quote_quantity,
                )
            ],
        )

        print("SELL", executed_quantity, base, "for", cumulative_quote_quantity, quote)

        return status

    async def buy_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.ExecutedOrder:
        quote_quantity = base_quantity * price
        executed_quantity = base_quantity * decimal.Decimal(1 - self.commission)
        cumulative_quote_quantity = quote_quantity

        assert self.holdings[quote] >= cumulative_quote_quantity

        self.holdings[base] = self.holdings.get(base, decimal.Decimal(0)) + executed_quantity
        self.holdings[quote] -= cumulative_quote_quantity
        if self.holdings[quote] == 0:
            del self.holdings[quote]

        status = trader.ExecutedOrder(
            base=base,
            quote=quote,
            side=trader.OrderSide.SELL,
            fills=[
                trader.Fill(
                    price=price,
                    base_quantity=executed_quantity,
                    quote_quantity=cumulative_quote_quantity,
                )
            ],
        )

        print("BUY", executed_quantity, base, "with", cumulative_quote_quantity, quote)

        return status

    async def sell_at(self, base: trader.Asset, quote: trader.Asset, price: trader.Price, base_quantity: trader.Quantity) -> trader.ExecutedOrder:
        quote_quantity = base_quantity * price
        executed_quantity = base_quantity
        cumulative_quote_quantity = quote_quantity * decimal.Decimal(1 - self.commission)

        assert self.holdings[base] >= executed_quantity

        self.holdings[base] -= executed_quantity
        if self.holdings[base] == 0:
            del self.holdings[base]
        self.holdings[quote] = self.holdings.get(quote, decimal.Decimal(0)) + cumulative_quote_quantity

        status = trader.ExecutedOrder(
            base=base,
            quote=quote,
            side=trader.OrderSide.SELL,
            fills=[
                trader.Fill(
                    price=price,
                    base_quantity=executed_quantity,
                    quote_quantity=cumulative_quote_quantity,
                )
            ],
        )

        print("SELL", executed_quantity, base, "for", cumulative_quote_quantity, quote)

        return status


@pytest.mark.asyncio
async def test_reblance() -> None:
    market = PlatonicMarket(
        holdings={
            "BTC": trader.Quantity("100.00000000"),
            "ETH": trader.Quantity("100.00000000"),
        },
        prices={
            ("ETH", "BTC"): trader.Price("1.00000000"),
            ("BNB", "BTC"): trader.Price("0.50000000"),
            ("DOGE", "BTC"): trader.Price("0.01000000"),
            ("BTC", "BUSD"): trader.Price("10.00"),
            ("ETH", "BUSD"): trader.Price("10.00"),
            ("BNB", "BUSD"): trader.Price("5.00"),
            ("DOGE", "BUSD"): trader.Price("0.10"),
            ("BTC", "BNB"): trader.Price("2.00000000"),
        },
        commission=0.001,
    )

    target = {
        "BNB": 0.75,
        "ETH": 0.1,
        "DOGE": 0.15,
    }
    minima: Dict[str, trader.Quantity] = {}

    quote = "BTC"
    value = "BUSD"
    threshold = 1.05

    await trader.rebalance(minima, target, value, quote, threshold, market)

    holdings = await market.get_holdings()
    values = {a: q * await market.get_price(a, value) for a, q in holdings.items()}
    total = sum(values.values())
    distribution = {k: float(v / total) for k, v in values.items()}

    assert all((d / target[k] if k in target else 1.0) < threshold for k, d in distribution.items()), distribution
    assert all(target.get(k, 0.0) / d < threshold for k, d in distribution.items()), distribution


@pytest.mark.asyncio
async def test_reblance_with_minima() -> None:
    initial_holdings = {
        "BTC": trader.Quantity("100.00000000"),
        "ETH": trader.Quantity("100.00000000"),
    }

    market = PlatonicMarket(
        holdings=initial_holdings,
        prices={
            ("ETH", "BTC"): trader.Price("1.00000000"),
            ("BNB", "BTC"): trader.Price("0.50000000"),
            ("DOGE", "BTC"): trader.Price("0.01000000"),
            ("XRP", "BTC"): trader.Price("0.10000000"),
            ("BTC", "BUSD"): trader.Price("10.00"),
            ("ETH", "BUSD"): trader.Price("10.00"),
            ("BNB", "BUSD"): trader.Price("5.00"),
            ("DOGE", "BUSD"): trader.Price("0.10"),
            ("XRP", "BUSD"): trader.Price("1.00"),
            ("BTC", "BNB"): trader.Price("2.00000000"),
        },
        commission=0.001,
    )

    target = {
        "BNB": 0.75,
        "ETH": 0.1,
        "DOGE": 0.15,
    }
    minima = {
        "BTC": trader.Quantity("10.00000000"),
        "XRP": trader.Quantity("10.00000000"),
    }

    quote = "BTC"
    value = "BUSD"
    threshold = 1.05

    await trader.rebalance(minima, target, value, quote, threshold, market)

    holdings = await market.get_holdings()

    non_quote_assets = set(initial_holdings) | set(holdings)
    non_quote_assets.remove(quote)

    for asset in minima:
        if asset != quote:
            # for most assets, worst case is two transfers
            expected = minima[asset] * decimal.Decimal(1 - 2 * market.commission)
            assert holdings[asset] >= expected, asset
        else:
            # for quote asset, we pay commission for every transfer in and out
            # worst case is every other asset transfers in and out
            expected = minima[asset] * decimal.Decimal(1 - 2 * len(non_quote_assets) * market.commission)
            assert holdings[asset] >= expected, asset
