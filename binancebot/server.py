from typing import Any, Iterator, List, TypeVar, Collection

import decimal
import itertools
import json

from aiohttp import web

from binancebot import trader


class MyJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        else:
            return json.JSONEncoder.default(self, o)


T = TypeVar("T")


class RingBuffer(Collection[T]):
    def __init__(self, size: int) -> None:
        self.items: List[T] = []
        self.start_index = 0
        self.size = size

    def __contains__(self, value: Any) -> bool:
        return value in self.items

    def __iter__(self) -> Iterator[T]:
        return itertools.chain(self.items[self.start_index:], self.items[:self.start_index])

    def __len__(self) -> int:
        return len(self.items)

    def push(self, value: T) -> None:
        if len(self) < self.size:
            self.items.append(value)
        else:
            self.items[self.start_index] = value
            self.start_index = (self.start_index + 1) % self.size


class DebugInfoServer:
    def __init__(self, trading_client: trader.TradingClient, logs: RingBuffer) -> None:
        self.app = web.Application()
        self.app.add_routes([web.get("/", self.home)])
        self.runner = web.AppRunner(self.app)
        self.site = None
        self.trading_client = trading_client
        self.logs = logs

    async def home(self, request: web.Request) -> web.Response:
        return web.Response(
            text=MyJSONEncoder(sort_keys=True).encode(
                dict(
                    message="everything is fine",
                    holdings=await self.trading_client.get_holdings(),
                    logs=list(self.logs),
                ),
            ),
            content_type="application/json",
        )

    async def start(self, host: str, port: int) -> None:
        assert self.site is None, "server has already been set up!"

        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host, port)
        assert self.site is not None
        await self.site.start()

    async def stop(self) -> None:
        await self.runner.cleanup()
        del self.site
