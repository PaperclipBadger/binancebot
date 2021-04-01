from typing import Awaitable, Callable, Type

import asyncio
import itertools
import contextlib

import httpx

from binancebot import binance, trader, server


QUOTE = "BUSD"
UPDATE_PERIOD = 5.0


async def suppress(exc_class: Type[BaseException], task: Awaitable):
    try:
        return await task
    except exc_class:
        return None


async def periodic(period: float, task: Callable[[], Awaitable]):
    loop = asyncio.get_running_loop()
    start = loop.time()

    for i in itertools.count(1):
        now = loop.time()
        delta = max(start + i * period - now, 0.0)
        await asyncio.gather(task(), asyncio.sleep(delta))


# API_BASE = "https://api.binance.com"
# API_KEY = "1LxUK4DDKBRKTftGryFicphhgnXSfZSGngJLTkpgF7nQMoHpJKWSlEcwDezaxoNY"
# SECRET_KEY = b"1xC6yn9paTsbHYQ9gNVjdzIIFADzUJD50ubjM3Ic4VS1mJwC4hpTRPpgqB6eaP2K"
API_BASE = "https://testnet.binance.vision"
API_KEY = "AAzcPjIao7mRjgfedz8yPgo42UjzgiUMbS2Xa8sDIdPwQ3nzAuqUvfJ7Dryycxd3"
SECRET_KEY = b"C12RUn34VFuj60yhLC4ogfuaPpcTonJ5sNrnRZgkvPi0e0HqT9MPj6QXipnSWFcg"

VALUE_ASSET = "USDT"
QUOTE_ASSET = "BTC"
TARGET_DISTRIBUTION = {
    "BTC": 0.25,
    "ETH": 0.25,
    "BNB": 0.25,
    "USDT": 0.25,
}
THRESHOLD = 1.1

loop = asyncio.get_event_loop()
http_client = httpx.AsyncClient()
trader_client = binance.BinanceClient(
    api_base=API_BASE,
    api_key=API_KEY,
    secret_key=SECRET_KEY,
    http_client=http_client,
)

loop.create_task(server.start("localhost", 8080))

main = loop.create_task(
    periodic(
        UPDATE_PERIOD,
        lambda: trader.rebalance(
            target=TARGET_DISTRIBUTION,
            value_asset=VALUE_ASSET,
            quote_asset=QUOTE_ASSET,
            threshold=THRESHOLD,
            client=trader_client,
        ),
    ),
)
loop.run_until_complete(main)

cleanup = loop.create_task(asyncio.gather(client.aclose(), server.stop()))
loop.run_until_complete(cleanup)
