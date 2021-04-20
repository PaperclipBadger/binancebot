import decimal
import json

from aiohttp import web

from binancebot import trader


class MyJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        else:
            return json.JSONEncoder.default(o)


class DebugInfoServer:
    def __init__(self, trading_client: trader.TradingClient):
        self.app = web.Application()
        self.app.add_routes([web.get("/", self.home)])
        self.runner = web.AppRunner(self.app)
        self.site = None
        self.trading_client = trading_client

    async def home(self, request: web.Request) -> web.Response:
        return web.Response(
            text=MyJSONEncoder(sort_keys=True).encode(
                dict(
                    message="everything is fine",
                    holdings=await self.trading_client.get_holdings(),
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
