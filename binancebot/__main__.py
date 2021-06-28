from typing import Awaitable, Callable, Mapping, Optional, Protocol, Sequence, Tuple, Type, TypeVar

import asyncio
import itertools
import logging
import math
import traceback

import httpx
import numpy as np

from binancebot import binance, trader, server


logger = logging.getLogger(__name__)


T = TypeVar("T")


async def suppress(exc_class: Type[BaseException], task: Awaitable[T]) -> Optional[T]:
    try:
        return await task
    except exc_class:
        logger.error(traceback.format_exc())
        return None


class Task(Protocol):
    def __call__(self, time: float, time_delta: float, step_count: int) -> Awaitable:
        ...


async def periodic(period: float, task: Task):
    loop = asyncio.get_running_loop()
    start = loop.time()

    for i in itertools.count(1):
        now = loop.time()
        delta = max(start + i * period - now, 0.0)
        await asyncio.gather(
            suppress(BaseException, task(
                time=now,
                time_delta=delta,
                step_count=i,
            )), asyncio.sleep(delta)
        )


def linear_least_squares(
    phis: Sequence[Callable[[float], float]],
    data: Sequence[Tuple[float, float]],
) -> Sequence[float]:
    xs, ys = zip(*data)
    X = np.array([[phi(x) for phi in phis] for x in xs], dtype=np.float64)
    Y = np.array(ys, dtype=np.float64).reshape(-1, 1)
    # https://en.wikipedia.org/wiki/Least_squares#Linear_least_squares
    return np.linalg.solve(X.T @ X, X.T @ Y).ravel()


async def target_distribution(
    assets: Sequence[trader.Asset],
    value_asset: trader.Asset,
    client: binance.BinanceClient,
    period_ms: int,
    window: binance.Interval,
    n_windows: int,
    beta: float,
) -> Mapping[trader.Asset, float]:
    """Weights assets higher based on the relative rate of change of their price."""
    async def get_price_change(asset: trader.Asset) -> float:
        if asset == value_asset:
            return 0.0

        candlesticks = await client.get_candlesticks(
            base=asset, quote=value_asset, period_ms=period_ms, interval=window,
        )

        def average_price(candlestick: binance.Candlestick) -> float:
            estimates = (
                candlestick.high,
                candlestick.low,
                candlestick.close,
            )
            return float(sum(estimates) / len(estimates))

        def t(candlestick: binance.Candlestick) -> float:
            return (
                (candlestick.close_time - candlesticks[0].close_time)
                / binance.Interval.ONE_DAY.milliseconds
            )

        # linear least squares fit in log space
        # i.e. y = e^(mx + c) = da^x
        phis = [lambda x: x, lambda x: 1.0]
        betas = linear_least_squares(
            phis, [(t(c), np.log(float(average_price(c)))) for c in candlesticks]
        )

        # log rate of change
        return betas[0]

        # def model(x):
        #     return np.exp(sum(beta * phi(x) for beta, phi in zip(betas, phis)))

        # # candlesticks are sorted so -1 is most recent
        # now = t(candlesticks[-1])
        # then = now - binance.Interval.ONE_DAY.milliseconds
        # change = model(now) / model(then)
        # return change - 1.0

    price_changes = await asyncio.gather(*map(get_price_change, assets))
    inputs = [5 * x if x < 0 else x for x in price_changes]

    # beta defines the sharpness of the softmax
    numerators = tuple(math.exp(beta * x) for x in inputs)
    denominator = sum(numerators)
    return {asset: numerator / denominator for asset, numerator in zip(assets, numerators)}


async def do_trading(
    assets: Sequence[trader.Asset],
    minima: Mapping[trader.Asset, trader.Quantity],
    value_asset: trader.Asset,
    quote_asset: trader.Asset,
    period_ms: int,
    window: binance.Interval,
    n_windows: int,
    beta: float,
    threshold: float,
    client: binance.BinanceClient,
    time: float = 0.0,  # timestamp in seconds
    time_delta: float = 0.0,  # time between this run and the last
) -> None:
    target = await target_distribution(
        assets=assets,
        value_asset=value_asset,
        client=client,
        period_ms=period_ms,
        window=window,
        n_windows=n_windows,
        beta=beta,
    )

    if time % 3600.0 < 1200.0 and (time - time_delta) % 3600.0 > 1200.0:
        logger.info("Target distribution:\n{}".format("\n".join(f"{k}:\t{v:.4f}" for k, v in target.items())))
    else:
        logger.debug("Target distribution:\n{}".format("\n".join(f"{k}:\t{v:.4f}" for k, v in target.items())))

    await trader.rebalance(
        target=target,
        minima=MINIMA,
        value_asset=VALUE_ASSET,
        quote_asset=QUOTE_ASSET,
        threshold=THRESHOLD,
        client=trader_client,
    )


API_BASE = "https://api.binance.com"
API_KEY = "1LxUK4DDKBRKTftGryFicphhgnXSfZSGngJLTkpgF7nQMoHpJKWSlEcwDezaxoNY"
SECRET_KEY = b"1xC6yn9paTsbHYQ9gNVjdzIIFADzUJD50ubjM3Ic4VS1mJwC4hpTRPpgqB6eaP2K"
# API_BASE = "https://testnet.binance.vision"
# API_KEY = "AAzcPjIao7mRjgfedz8yPgo42UjzgiUMbS2Xa8sDIdPwQ3nzAuqUvfJ7Dryycxd3"
# SECRET_KEY = b"C12RUn34VFuj60yhLC4ogfuaPpcTonJ5sNrnRZgkvPi0e0HqT9MPj6QXipnSWFcg"

http_client = httpx.AsyncClient()
trader_client = binance.BinanceClient(
    api_base=API_BASE,
    api_key=API_KEY,
    secret_key=SECRET_KEY,
    http_client=http_client,
)

VALUE_ASSET = "USDT"
QUOTE_ASSET = "BTC"

ASSETS = [
    # Fiat (should be stable)
    VALUE_ASSET,
    # Bitcoin and friends
    "BTC",
    "BCH",
    "LTC",
    # Ethereum and competitors
    "ETH",
    "ETC",
    "ADA",
    "DOT",
    "TRX",
    # Privacy
    "XMR",
    "ZEC",
    # Utility coins
    "IOTA",
    "FIL",
    "LINK",
    "BAT",
    # Finance
    "XRP",
    "XLM",
    "BNB",
    "UNI",
    "AAVE",
    # Meme value
    "DOGE",
]

# TARGET_DISTRIBUTION = {
#     # Bitcoin and friends
#     "BTC": 0.1,
#     "BCH": 0.025,
#     "LTC": 0.025,
#     # Ethereum and competitors
#     "ETH": 0.5,
#     "ETC": 0.025,
#     # "ADA": 0.025,
#     "DOT": 0.025,
#     # "TRX": 0.025,
#     # Privacy
#     "XMR": 0.1,
#     # "ZEC": 0.025,
#     # Utility coins
#     # "IOTA"
#     # "FIL": 0.0125,
#     "LINK": 0.025,
#     # "BAT": 0.0125,
#     # Finance
#     "XRP": 0.025,
#     "XLM": 0.025,
#     "BNB": 0.025,
#     # "UNI": 0.0125,
#     # "AAVE": 0.0125,
#     # Meme value
#     "DOGE": 0.1,
# }

MINIMA = {
    # used to pay exchange fees
    "BNB": trader.Quantity("0.1000000"),
    # quote currency, need a margin to account for fees
    "BTC": trader.Quantity("0.0010000"),
    # also sometimes used as a quote currency
    "USDT": trader.Quantity("30.00000000")
}
PERIOD_MS = binance.Interval.ONE_DAY.milliseconds
WINDOW = binance.Interval.FIVE_MINUTES
N_WINDOWS = 15
BETA = 10.0
THRESHOLD = 0.01
UPDATE_PERIOD = 30.0

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s")

file_handler = logging.FileHandler("binancebot.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

root_logger.info("(re)started")

loop = asyncio.get_event_loop()

debug_server = server.DebugInfoServer(trader_client)
loop.create_task(debug_server.start("localhost", 8080))

main = loop.create_task(
    periodic(
        UPDATE_PERIOD,
        lambda time, time_delta, step_count: do_trading(
            assets=ASSETS,
            minima=MINIMA,
            value_asset=VALUE_ASSET,
            quote_asset=QUOTE_ASSET,
            threshold=THRESHOLD,
            period_ms=PERIOD_MS,
            beta=BETA,
            window=WINDOW,
            n_windows=N_WINDOWS,
            client=trader_client,
            time=time,
            time_delta=time_delta,
        ),
    ),
)
loop.run_until_complete(main)

cleanup = loop.create_task(asyncio.gather(http_client.aclose(), debug_server.stop()))
loop.run_until_complete(cleanup)
