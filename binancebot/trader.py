from typing import Dict, List, Mapping, Optional, Sequence

import asyncio
import dataclasses
import decimal
import enum
import itertools
import logging


logger = logging.getLogger(__name__)


Asset = str
Quantity = decimal.Decimal
Price = decimal.Decimal


class OrderSide(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclasses.dataclass(frozen=True)
class Fill:
    price: Price
    base_quantity: Quantity
    quote_quantity: Quantity


@dataclasses.dataclass(frozen=True)
class ExecutedOrder:
    base: Asset
    quote: Asset
    side: OrderSide
    fills: Sequence[Fill]

    @property
    def base_quantity(self):
        return sum(fill.base_quantity for fill in self.fills)

    @property
    def quote_quantity(self):
        return sum(fill.quote_quantity for fill in self.fills)


@dataclasses.dataclass(frozen=True)
class MarketOrder:
    base: Asset
    quote: Asset
    base_quantity: Optional[Quantity] = None
    quote_quantity: Optional[Quantity] = None


@dataclasses.dataclass(frozen=True)
class LimitOrder:
    base: Asset
    quote: Asset
    price: Price
    base_quantity: Quantity


class OrderError(ValueError):
    pass


class TradingClient:
    async def get_holdings(self) -> Mapping[Asset, Quantity]:
        ...

    async def get_price(self, base: Asset, quote: Asset) -> Price:
        ...

    async def apply_market_filters(
        self,
        base: Asset,
        quote: Asset,
        *,
        base_quantity: Quantity = None,
        quote_quantity: Quantity = None,
    ) -> List[MarketOrder]:
        ...

    async def apply_limit_filters(
        self,
        base: Asset,
        quote: Asset,
        price: Price,
        base_quantity: Quantity,
    ) -> List[LimitOrder]:
        ...

    async def buy_at_market(
        self,
        base: Asset,
        quote: Asset,
        *,
        base_quantity: Quantity = None,
        quote_quantity: Quantity = None,
    ) -> ExecutedOrder:
        ...

    async def sell_at_market(
        self,
        base: Asset,
        quote: Asset,
        *,
        base_quantity: Quantity = None,
        quote_quantity: Quantity = None,
    ) -> ExecutedOrder:
        ...

    async def buy_at(self, base: Asset, quote: Asset, price: Price, base_quantity: Quantity) -> ExecutedOrder:
        ...

    async def sell_at(self, base: Asset, quote: Asset, price: Price, base_quantity: Quantity) -> ExecutedOrder:
        ...


async def rebalance(
    minima: Mapping[Asset, Quantity],
    target: Mapping[Asset, float],
    value_asset: Asset,
    quote_asset: Asset,
    threshold: float,
    client: TradingClient,
) -> None:
    assert abs(sum(target.values()) - 1.0) < 1.e-12

    holdings = dict(await client.get_holdings())
    del holdings["NFT"]  # hotfix: there are no NFT markets on binance
    logger.debug(f"holdings: {holdings}")
    base_assets = set(target) | set(holdings) | set(minima)

    pairs = [
        (base, quote)
        for base in base_assets
        for quote in (quote_asset, value_asset)
        if base != quote
        if base != value_asset
    ]

    if value_asset in base_assets and (quote_asset, value_asset) not in pairs:
        pairs.append((quote_asset, value_asset))

    prices = await asyncio.gather(*(client.get_price(base, quote) for base, quote in pairs))
    market = {pair: price for pair, price in zip(pairs, prices)}

    assert holdings

    def get_price(base: Asset, quote: Asset) -> Price:
        if base == quote:
            return decimal.Decimal(1)
        elif base == value_asset:
            return 1 / market[quote, value_asset]
        elif quote == value_asset:
            return market[base, value_asset]
        elif base == quote_asset:
            return 1 / market[quote, quote_asset]
        elif quote == quote_asset:
            return market[base, quote_asset]
        else:
            return market[base, quote_asset] / market[quote, quote_asset]

    def get_value(asset: Asset, quantity: Quantity = None) -> Quantity:
        if quantity is None:
            quantity = holdings.get(asset, Quantity(0))
        assert quantity is not None
        return quantity * get_price(asset, value_asset)

    # value distribution of portfolio
    all_assets = sorted(set(holdings) | set(target) | set(minima))
    values = {asset: get_value(asset) for asset in all_assets}
    total = sum(values.values())

    distribution = {
        k: v / total
        for k, v in values.items()
    }

    logger.debug(f"current distribution: {', '.join('{}={:.3f}'.format(k, v) for k, v in distribution.items())}")

    unreserved_assets = set(target)
    unreserved_sum = 1.0

    target_ = dict(target)

    for asset in minima:
        minimum_t = float(get_value(asset, minima.get(asset, Quantity(0))) / total)
        t = target_.get(asset, 0.0)
        if minimum_t > t:
            assert unreserved_sum >= minimum_t

            target_[asset] = minimum_t
            unreserved_assets.discard(asset)
            rescaler = (unreserved_sum - minimum_t) / (unreserved_sum - t)
            for k in unreserved_assets:
                target_[k] *= rescaler

            unreserved_sum -= minimum_t

    target = target_

    def delta(k: Asset) -> float:
        # we once did this in log space, but it works poorly for small values
        return float(distribution.get(k, 0.0)) - target.get(k, 0.0)

    # entry b: q means sell q of your b for quote asset
    sells: Dict[Asset, Quantity] = {}
    # entry a: q means buy q of a with quote asset
    buys: Dict[Asset, Quantity] = {}

    # sorted list of assets by how badly misallocated they are
    by_allocation = sorted(zip(map(delta, distribution), distribution))

    # implement a greedy partial rebalancing.
    # While the portfolio is underbalanced, sell the most overrepresented asset
    # in exchange for the most underrepresented asset such that one or the other
    # become perfectly represented.
    # This way we rebalance using at most len(holdings) + len(target) trades,
    # since at every step of the loop we balance some asset.
    for _, asset in by_allocation:
        logger.debug(
            f"asset: {asset}"
            f", holdings: {holdings.get(asset, Quantity(0))}"
            f", value: {values[asset]}"
            f", distribution: {distribution[asset]}"
        )

    holdings_ = holdings.copy()
    distribution_ = distribution.copy()

    while by_allocation[0][0] < -threshold or by_allocation[-1][0] > threshold:
        _, under = by_allocation[0]
        _, over = by_allocation[-1]

        recovered = min(
            decimal.Decimal(target[under]) - distribution[under],
            distribution[over] - decimal.Decimal(target.get(over, 0)),
        )
        over_quantity = holdings[over] * (recovered / distribution[over])
        under_quantity = over_quantity * get_price(over, under)

        logger.debug(f"{over=}, {under=}")
        logger.debug(f"{over_quantity=}")
        logger.debug(f"{under_quantity=}")
        logger.debug(f"{get_price(under, over)=}")

        # do all trading via a quote asset, so we don't use any low-liquidity altcoin markets
        assert under != over
        if under == quote_asset:
            sells.setdefault(over, Quantity(0))
            sells[over] += over_quantity
        elif over == quote_asset:
            buys.setdefault(under, Quantity(0))
            buys[under] += under_quantity
        else:
            sells.setdefault(over, Quantity(0))
            sells[over] += over_quantity
            buys.setdefault(under, Quantity(0))
            buys[under] += under_quantity

        assert over_quantity <= holdings[over]
        holdings[over] -= over_quantity
        holdings[under] = holdings.get(under, Quantity(0)) + under_quantity

        assert recovered <= distribution[over]
        distribution[over] -= recovered
        distribution[under] += recovered

        by_allocation[0] = delta(under), under
        by_allocation[-1] = delta(over), over
        by_allocation.sort()  # timsort, don't fail me now

    logger.debug(f"planned sells: {sells}")
    logger.debug(f"planned buys: {buys}")
    logger.debug(f"holdings after planned trades: {holdings}")
    logger.debug(f"distribution after planned trades: {distribution}")

    holdings = holdings_
    distribution = distribution_

    # convert planned trades into legal trades
    real_sellss: List[List[MarketOrder]]
    real_buyss: List[List[MarketOrder]]
    real_sellss, real_buyss = await asyncio.gather(
        asyncio.gather(
            *(
                (
                    client.apply_market_filters(
                        base=base_asset,
                        quote=quote_asset,
                        base_quantity=base_quantity,
                    )
                    if base_asset != value_asset
                    else client.apply_market_filters(
                        base=quote_asset,
                        quote=value_asset,
                        quote_quantity=base_quantity,
                    )
                )
                for base_asset, base_quantity in sells.items()
            ),
        ),
        asyncio.gather(
            *(
                (
                    client.apply_market_filters(
                        base=base_asset,
                        quote=quote_asset,
                        base_quantity=base_quantity,
                    )
                    if base_asset != value_asset
                    else client.apply_market_filters(
                        base=quote_asset,
                        quote=value_asset,
                        quote_quantity=base_quantity,
                    )
                )
                for base_asset, base_quantity in buys.items()
            ),
        ),
    )

    logger.debug(f"{real_sellss=}")
    logger.debug(f"{real_buyss=}")

    holdings_ = holdings.copy()

    # dry run the real trades and check that we have sufficient funds
    for sell in itertools.chain.from_iterable(real_sellss):
        if sell.quote == quote_asset:
            assert sell.base_quantity is not None
            assert sell.quote_quantity is None
            quote_quantity = sell.base_quantity * get_price(sell.base, sell.quote)

            if holdings[sell.base] < sell.base_quantity:
                logger.error(f"base holdings {holdings[sell.base]}{sell.base} should be greater than {sell.base_quantity}")  # noqa: E127

            holdings[sell.base] -= sell.base_quantity
            holdings.setdefault(sell.quote, Quantity(0))
            holdings[sell.quote] += quote_quantity
        elif sell.quote == value_asset:
            assert sell.base_quantity is None
            assert sell.quote_quantity is not None
            base_quantity = sell.quote_quantity * get_price(sell.quote, sell.base)

            if holdings[sell.quote] < sell.quote_quantity:
                logger.error(f"quote holdings {holdings[sell.quote]}{sell.quote} should be greater than {sell.quote_quantity}")  # noqa: E127

            holdings.setdefault(sell.base, Quantity(0))
            holdings[sell.base] += base_quantity
            holdings[sell.quote] -= sell.quote_quantity
        else:
            raise RuntimeError("Inconceivable!")

    for buy in itertools.chain.from_iterable(real_buyss):
        if buy.quote == quote_asset:
            assert buy.base_quantity is not None
            assert buy.quote_quantity is None
            quote_quantity = buy.base_quantity * get_price(buy.base, buy.quote)

            if holdings[buy.quote] < quote_quantity:
                logger.error(f"quote holdings {holdings[buy.quote]}{buy.quote} should be greater than {quote_quantity}")  # noqa: E127

            holdings.setdefault(buy.base, Quantity(0))
            holdings[buy.base] += buy.base_quantity
            holdings[buy.quote] -= quote_quantity
        elif buy.quote == value_asset:
            assert buy.base_quantity is None
            assert buy.quote_quantity is not None
            base_quantity = buy.quote_quantity * get_price(buy.quote, buy.base)

            if holdings[buy.base] < base_quantity:
                logger.error(f"base holdings {holdings[buy.base]}{buy.base} should be greater than {base_quantity}")  # noqa: E127

            holdings[buy.base] -= base_quantity
            holdings.setdefault(buy.quote, Quantity(0))
            holdings[buy.quote] += buy.quote_quantity
        else:
            raise RuntimeError("Inconceivable!")

    # do the trades!

    for sell in itertools.chain.from_iterable(real_sellss):
        if sell.quote == quote_asset:
            logger.info(f"selling {sell.base_quantity} {sell.base} for {sell.quote}")
        elif sell.quote == value_asset:
            logger.info(f"buying {sell.base} with {sell.quote_quantity} {sell.quote}")
        else:
            raise RuntimeError("Inconceivable!")

    sell_results = await asyncio.gather(
        *(
            (
                client.sell_at_market(**dataclasses.asdict(sell))
                if sell.quote != value_asset
                else client.buy_at_market(**dataclasses.asdict(sell))
            )
            for real_sells in real_sellss
            for sell in real_sells
        ),
        return_exceptions=True,
    )

    for r, sell in zip(sell_results, itertools.chain.from_iterable(real_sellss)):
        if isinstance(r, BaseException):
            if sell.quote == quote_asset:
                logger.error(
                    f"Error selling {sell.base_quantity} {sell.base} for {sell.quote}:"
                    f" {r.__class__.__name__}: {r}"
                )
            elif sell.quote == value_asset:
                logger.error(
                    f"Error buying {sell.base} with {sell.quote_quantity} {sell.quote}:"
                    f" {r.__class__.__name__}: {r}"
                )
            else:
                raise RuntimeError("Inconceivable!")

    for buy in itertools.chain.from_iterable(real_buyss):
        if buy.quote == quote_asset:
            logger.info(f"buying {buy.base_quantity} {buy.base} with {buy.quote}")
        elif buy.quote == value_asset:
            logger.info(f"selling {buy.base} for {buy.quote_quantity} {buy.quote}")
        else:
            raise RuntimeError("Inconceivable!")

    buy_results = await asyncio.gather(
        *(
            (
                client.buy_at_market(**dataclasses.asdict(buy))
                if buy.quote != value_asset
                else client.sell_at_market(**dataclasses.asdict(buy))
            )
            for real_buys in real_buyss
            for buy in real_buys
        ),
        return_exceptions=True,
    )

    for r, buy in zip(buy_results, itertools.chain.from_iterable(real_buyss)):
        if isinstance(r, BaseException):
            if buy.quote == quote_asset:
                logger.error(
                    f"Error buying {buy.base_quantity} {buy.base} with {buy.quote}:"
                    f" {r.__class__.__name__}: {r}"
                )
            elif buy.quote == value_asset:
                logger.error(
                    f"Error selling {buy.base} for {buy.quote_quantity} {buy.quote}:"
                    f" {r.__class__.__name__}: {r}"
                )
            else:
                raise RuntimeError("Inconceivable!")


# do all trading via a quote asset, so we don't use any low-liquidity altcoin markets
async def sell_via(base, quote, via, base_quantity, client) -> None:
    assert base != quote
    logger.info(f"trading {base_quantity} {base} for {quote} via {via}")

    if base == via:
        logger.debug("base==via, direct trade")
        await client.buy_at_market(
            quote,
            base,
            quote_quantity=base_quantity,
        )
    elif via == quote:
        logger.debug("via==quote, direct trade")
        await client.sell_at_market(
            base,
            quote,
            base_quantity=base_quantity,
        )
    else:
        logger.debug("indirect trade")
        logger.info(f"trading {base_quantity} {base} for {via}")
        sell_result = await client.sell_at_market(
            base,
            via,
            base_quantity=base_quantity,
        )
        via_quantity = sell_result.quote_quantity
        logger.info(f"trading {via_quantity} {via} for {quote}")
        await client.buy_at_market(
            quote,
            via,
            quote_quantity=via_quantity,
        )
