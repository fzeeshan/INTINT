import asyncio
from aiohttp import web

class IntServer:
    def __init__(self, q):
        self.app = web.Application()
        self.app.router.add_get('/api/', self.handler)
        self.app.router.add_get('/api/source/{sourceid}/start', self.handle_source)
        self.q = q  # Queue to communicate with data server

    def run(self):
        web.run_app(self.app, port=6543, host='ST71532')

    async def handler(self, request):
        data = {'some': str([str(r) for r in request])}
        return web.json_response(data)

    async def handle_source(self, request):
        self.q.put({'sourceid': request.match_info['sourceid'], 'action':0, 'mode':'sourceid'})
        return web.Response(text='OK')
