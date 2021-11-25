import hashlib

import aiohttp
from aiohttp import web
from server.request_handler import RequestHandler


class RouterRequestHandler(RequestHandler):
    def __init__(self, servers: list[str]):
        self.servers = servers

    async def handle_get_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        int_hash = get_key_int_hash(key)
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://{self.servers[int_hash % len(self.servers)]}/{dbname}/{key}') as resp:
                value = await resp.text()
                if value:
                    return web.Response(text=value)
        return web.Response(status=404)

    async def handle_post_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        int_hash = get_key_int_hash(key)
        async with aiohttp.ClientSession() as session:
            async with session.post(f'http://{self.servers[int_hash % len(self.servers)]}/{dbname}/{key}',
                                    data=await request.read()) as resp:

                return web.Response(status=resp.status)

    async def handle_delete_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        int_hash = get_key_int_hash(key)
        async with aiohttp.ClientSession() as session:
            async with session.delete(f'http://{self.servers[int_hash % len(self.servers)]}/{dbname}/{key}') as resp:
                return web.Response(status=resp.status)

def get_key_int_hash(key: str) -> int:
    return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % 10 ** 8
