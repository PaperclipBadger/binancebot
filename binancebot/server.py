import json

from aiohttp import web


routes = web.RouteTableDef()


@routes.get("/")
async def hello(request):
    return web.Response(text=json.dumps(dict(message="hello")), content_type="application/json")


@routes.get("/holdings")
async def holdings(request):
    return web.Response(text=json.dumps(dict(message="hello")), content_type="application/json")


app = web.Application()
app.add_routes(routes)
runner = web.AppRunner(app)


async def start(host, port):
    global runner
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()


async def stop():
    global runner
    await runner.cleanup()
