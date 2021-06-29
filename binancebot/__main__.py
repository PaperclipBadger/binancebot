from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol, Sequence, Tuple, Type, TypeVar

import argparse
import asyncio
import itertools
import logging
import logging.handlers
import math
import traceback

import httpx
import numpy as np
import toml

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
        beta=beta,
    )

    if time % 3600.0 < 1200.0 and (time - time_delta) % 3600.0 > 1200.0:
        logger.info("Target distribution:\n{}".format("\n".join(f"{k}:\t{v:.4f}" for k, v in target.items())))
    else:
        logger.debug("Target distribution:\n{}".format("\n".join(f"{k}:\t{v:.4f}" for k, v in target.items())))

    await trader.rebalance(
        target=target,
        minima=minima,
        value_asset=value_asset,
        quote_asset=quote_asset,
        threshold=threshold,
        client=trader_client,
    )


parser = argparse.ArgumentParser()
parser.add_argument("--config-file", default="config.toml")
args = parser.parse_args()

with open(args.config_file) as f:
    config = toml.load(f)

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s")

file_handler = logging.handlers.TimedRotatingFileHandler(
    config.get("logging", {}).get("file", "binancebot.log"),
    when="midnight",
    backupCount=7,
)
file_handler.setLevel(getattr(logging, config.get("logging", {}).get("file_level", "debug").upper()))
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, config.get("logging", {}).get("console_level", "info").upper()))
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)


class ServerHandler(logging.handlers.QueueHandler):
    def __init__(self, queue: server.RingBuffer):
        logging.handlers.QueueHandler.__init__(self, queue)  # type: ignore

    def prepare(self, record):
        return dict(
            time=record.asctime,
            level=record.levelname,
            name=record.name,
            message=record.message,
        )

    def enqueue(self, record):
        self.queue.push(record)


server_buffer: server.RingBuffer[Any]
server_buffer = server.RingBuffer(size=config.get("logging", {}).get("server_num", 100))
server_handler = ServerHandler(server_buffer)
server_handler.setLevel(getattr(logging, config.get("logging", {}).get("server_level", "info").upper()))
server_handler.setFormatter(formatter)
root_logger.addHandler(server_handler)

root_logger.info("(re)started")

http_client = httpx.AsyncClient()
trader_client = binance.BinanceClient(
    api_base=config["binance"]["api_base"],
    api_key=config["binance"]["api_key"],
    secret_key=config["binance"]["secret_key"].encode("ascii"),
    http_client=http_client,
)

loop = asyncio.get_event_loop()

debug_server = server.DebugInfoServer(trader_client, server_buffer)
loop.create_task(debug_server.start(config["server"]["host"], config["server"]["port"]))

main = loop.create_task(
    periodic(
        config["trader"]["update_period_s"],
        lambda time, time_delta, step_count: do_trading(
            assets=config["trader"]["traded_assets"],
            minima={k: trader.Quantity(v) for k, v in config["trader"]["minima"].items()},
            value_asset=config["trader"]["value_asset"],
            quote_asset=config["trader"]["quote_asset"],
            threshold=config["trader"]["threshold"],
            period_ms=config["trader"]["history_window_ms"],
            window=binance.Interval(config["trader"]["history_resolution"]),
            beta=config["trader"]["beta"],
            client=trader_client,
            time=time,
            time_delta=time_delta,
        ),
    ),
)
loop.run_until_complete(main)

cleanup = loop.create_task(asyncio.gather(http_client.aclose(), debug_server.stop()))
loop.run_until_complete(cleanup)
