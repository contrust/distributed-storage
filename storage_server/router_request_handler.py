import aiohttp
from aiohttp import web

from consistent_hashing.hash_ring import HashRing
from storage_server.request_handler import RequestHandler


class RouterRequestHandler(RequestHandler):
    def __init__(self, hash_ring: HashRing):
        self.hash_ring = hash_ring
        self.nodes = list(hash_ring.nodes.keys())

    async def handle_get_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_string(key)
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://{node}/{dbname}/{key}') as resp:
                value = await resp.text()
                if value:
                    return web.Response(text=value)
            for replica in self.hash_ring.nodes_replicas[node]:
                async with session.get(
                        f'http://{replica}/{dbname}/{key}') as resp:
                    value = await resp.text()
                    if value:
                        return web.Response(text=value)
        return web.Response(status=404)

    async def handle_post_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_string(key)
        next_node = self.nodes[(self.nodes.index(node) + 1) % len(self.nodes)]
        responses = []
        async with aiohttp.ClientSession() as session:
            async with session.post(f'http://{node}/{dbname}/{key}',
                                    data=await request.read()) as resp:
                responses.append(resp.status)
            for replica in self.hash_ring.nodes_replicas[node]:
                async with session.post(f'http://{replica}/{dbname}/{key}',
                                        data=await request.read()) as resp:
                    responses.append(resp.status)
        return web.Response(status=(200 if 200 in responses else 400))

    async def handle_delete_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_string(key)
        responses = []
        async with aiohttp.ClientSession() as session:
            async with session.delete(
                    f'http://{node}/{dbname}/{key}') as resp:
                responses.append(resp.status)
            for replica in self.hash_ring.nodes_replicas[node]:
                async with session.delete(
                        f'http://{replica}/{dbname}/{key}') as resp:
                    responses.append(resp.status)
        return web.Response(status=(200 if 200 in responses else 400))

    async def handle_patch_request(self, request: aiohttp.request):
        return web.Response(status=405)
