import hashlib

import aiohttp
from aiohttp import web

from consistent_hashing.hash_ring import HashRing
from storage_server.request_handler import RequestHandler


class RouterRequestHandler(RequestHandler):
    def __init__(self, hash_ring: HashRing):
        self.hash_ring = hash_ring

    async def handle_get_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_bytes(key)
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://{node}/{dbname}/{key}') as resp:
                value = await resp.text()
                if value:
                    return web.Response(text=value)
        return web.Response(status=404)

    async def handle_post_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_bytes(key)
        async with aiohttp.ClientSession() as session:
            async with session.post(f'http://{node}/{dbname}/{key}',
                                    data=await request.read()) as resp:
                return web.Response(status=resp.status)

    async def handle_delete_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_bytes(key)
        async with aiohttp.ClientSession() as session:
            async with session.delete(
                    f'http://{node}/{dbname}/{key}') as resp:
                return web.Response(status=resp.status)
