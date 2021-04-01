from typing import Any, Mapping

import asyncio
import enum
import dataclasses
import decimal
import math


Asset = str
Quantity = decimal.Decimal
Price = decimal.Decimal


class OrderSide(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


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


@dataclasses.dataclass(frozen=True)
class OrderInfo:
    order_id: Any
    base: Asset
    quote: Asset
    side: OrderSide
    original_quantity: Quantity
    executed_quantity: Quantity
    cumulative_quote_quantity: Quantity
    status: OrderStatus


class TradingClient:
    async def get_holdings(self) -> Mapping[Asset, Quantity]:
        ...

    async def get_price(self, base: Asset, quote: Asset) -> Price:
        ...

    async def get_order_info(self, order_id: Any) -> OrderInfo:
        ...

    async def buy_at_market(
        self,
        base: Asset,
        quote: Asset,
        *,
        base_quantity: Quantity = None,
        quote_quantity: Quantity = None,
    ) -> OrderInfo:
        ...

    async def sell_at_market(
        self,
        base: Asset,
        quote: Asset,
        *,
        base_quantity: Quantity = None,
        quote_quantity: Quantity = None,
    ) -> OrderInfo:
        ...

    async def buy_at(self, base: Asset, quote: Asset, price: Price, base_quantity: Quantity) -> OrderInfo:
        ...

    async def sell_at(self, base: Asset, quote: Asset, price: Price, base_quantity: Quantity) -> OrderInfo:
        ...


async def rebalance(
    target: Mapping[Asset, float],
    value_asset: Asset,
    quote_asset: Asset,
    threshold: float,
    client: TradingClient,
) -> None:
    assert abs(sum(target.values()) - 1.0) < 1.e-12

    holdings = dict(await client.get_holdings())
    base_assets = set(target) | set(holdings)

    pairs = [
        (base, quote)
        for base in base_assets
        for quote in (quote_asset, value_asset)
        if base != quote
    ]
    prices = await asyncio.gather(*(client.get_price(base, quote) for base, quote in pairs))
    market = {pair: price for pair, price in zip(pairs, prices)}

    assert holdings

    def get_price(base: Asset, quote: Asset) -> Price:
        if base == quote:
            return decimal.Decimal(1)
        elif base == quote_asset:
            return 1 / market[quote, quote_asset]
        elif quote == quote_asset:
            return market[base, quote_asset]
        elif base == value_asset:
            return 1 / market[quote, value_asset]
        elif quote == value_asset:
            return market[base, value_asset]
        else:
            return market[base, quote_asset] / market[quote, quote_asset]

    def get_value(asset: Asset):
        quantity = holdings.get(asset, 0)
        return quantity * get_price(asset, value_asset)

    # value distribution of portfolio
    all_assets = sorted(set(holdings) | set(target))
    values = {asset: get_value(asset) for asset in all_assets}
    total = sum(values.values())

    distribution = {
        k: v / total
        for k, v in values.items()
    }

    def delta(k: Asset) -> float:
        if distribution[k] > 0 and k in target:
            return math.log(distribution[k]) - math.log(target[k])
        elif distribution[k] > 0 and k not in target:
            return float("inf")
        elif distribution[k] == 0 and k in target:
            return float("-inf")
        elif distribution[k] == 0 and k not in target:
            return 0.0
        else:
            assert False, "inconceivable!"

    # list of triples (q, b, a) meaning sell q of your b for a
    trades = []

    # sorted list of assets by how badly misallocated they are
    by_allocation = sorted(zip(map(delta, distribution), distribution))

    # implement a greedy partial rebalancing.
    # While the portfolio is underbalanced, sell the most overrepresented asset
    # in exchange for the most underrepresented asset such that one or the other
    # become perfectly represented.
    # This way we rebalance using at most len(holdings) + len(target) trades,
    # since at every step of the loop we balance some asset.
    log_threshold = math.log(threshold)

    while by_allocation[0][0] < -log_threshold or by_allocation[-1][0] > log_threshold:
        _, under = by_allocation[0]
        _, over = by_allocation[-1]

        recovered = min(
            decimal.Decimal(target[under]) - distribution[under],
            distribution[over] - decimal.Decimal(target.get(over, 0)),
        )

        over_quantity = holdings[over] * (recovered / distribution[over])
        under_quantity = over_quantity * get_price(under, over)

        trades.append((over_quantity, over, under))

        assert over_quantity <= holdings[over]
        holdings[over] -= over_quantity
        holdings[under] = holdings.get(under, Quantity(0)) + under_quantity

        assert recovered <= distribution[over]
        distribution[over] -= recovered
        distribution[under] += recovered

        by_allocation[0] = delta(under), under
        by_allocation[-1] = delta(over), over
        by_allocation.sort()  # timsort, don't fail me now

    await asyncio.gather(
        *(
            buy_via(sell, buy, quote_asset, base_quantity, client)
            for base_quantity, sell, buy in trades
        )
    )


# do all trading via a quote asset, so we don't use any low-liquidity altcoin markets
async def buy_via(base, quote, via, base_quantity, client) -> None:
    assert base != quote

    if base == via:
        result = await client.buy_at_market(
            quote,
            base,
            quote_quantity=base_quantity,
        )
    elif via == quote:
        result = client.sell_at_market(
            base,
            quote,
            base_quantity=base_quantity,
        )
    else:
        sell_result = await client.sell_at_market(
            base,
            via,
            base_quantity=base_quantity,
        )

        if sell_result.status.is_open:
            sell_order = await client.get_order_info(sell_result.order_id)
            while sell_order.status.is_open:
                await asyncio.sleep(0.5)
                sell_order = await client.get_order_info(result.order_id)

            via_quantity = sell_order.cumulative_quote_quantity
        else:
            via_quantity = sell_result.cumulative_quote_quantity

        result = await client.buy_at_market(
            quote,
            via,
            quote_quantity=via_quantity,
        )

    if result.status.is_open:
        order = await client.get_order_info(result.order_id)
        while order.status.is_open:
            await asyncio.sleep(0.5)
            order = await client.get_order_info(result.order_id)
