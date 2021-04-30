from typing import Awaitable, Callable, Type

import asyncio
import itertools
import logging

import httpx

from binancebot import binance, trader, server


logger = logging.getLogger(__name__)


async def suppress(exc_class: Type[BaseException], task: Awaitable):
    try:
        return await task
    except exc_class as e:
        logger.error(f"{e.__class__.__name__}: {e}")
        return None


async def periodic(period: float, task: Callable[[], Awaitable]):
    loop = asyncio.get_running_loop()
    start = loop.time()

    for i in itertools.count(1):
        now = loop.time()
        delta = max(start + i * period - now, 0.0)
        await asyncio.gather(
            suppress(BaseException, task()), asyncio.sleep(delta)
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
TARGET_DISTRIBUTION = {
    # Bitcoin and friends
    "BTC": 0.05,
    "BCH": 0.05,
    "LTC": 0.05,
    # Ethereum and competitors
    "ETH": 0.05,
    "ETC": 0.05,
    "ADA": 0.05,
    "DOT": 0.05,
    "TRX": 0.05,
    # Privacy
    "XMR": 0.05,
    "ZEC": 0.05,
    # Utility coins
    "IOTA": 0.05,
    "FIL": 0.05,
    "LINK": 0.05,
    "BAT": 0.05,
    # Finance
    "XRP": 0.05,
    "XLM": 0.05,
    "BNB": 0.05,
    "UNI": 0.05,
    "AAVE": 0.05,
    # Meme value
    "DOGE": 0.05,
}
MINIMA = {
    # used to pay exchange fees
    "BNB": trader.Quantity("0.1000000"),
}
THRESHOLD = 1.1
UPDATE_PERIOD = 60.0


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
        lambda: trader.rebalance(
            minima=MINIMA,
            target=TARGET_DISTRIBUTION,
            value_asset=VALUE_ASSET,
            quote_asset=QUOTE_ASSET,
            threshold=THRESHOLD,
            client=trader_client,
        ),
    ),
)
loop.run_until_complete(main)

cleanup = loop.create_task(asyncio.gather(http_client.aclose(), debug_server.stop()))
loop.run_until_complete(cleanup)
