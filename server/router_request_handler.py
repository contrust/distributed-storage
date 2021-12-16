import json

import aiohttp
import requests
from aiohttp import web

from hashring.hashring import HashRing
from server.request_handler import RequestHandler


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
        message = await request.json()
        message['keys'] = {int(x): y for x, y in message['keys'].items()}
        new_ring = HashRing()
        new_ring.__dict__.update(message)
        old_ring = self.hash_ring
        old_nodes = list(old_ring.nodes.keys())
        new_nodes = list(new_ring.nodes.keys())
        old_hosts_ranges = old_ring.get_nodes_ranges()
        old_hosts_replicas = old_ring.nodes_replicas
        new_hosts_ranges = new_ring.get_nodes_ranges()
        new_hosts_replicas = new_ring.nodes_replicas
        for host in old_nodes:
            for replica in old_hosts_replicas[host]:
                message = {'method': 'add_ranges_to_host',
                           'host': host,
                           'ranges': old_hosts_ranges[host]}
                requests.patch(f'http://{replica}/', json.dumps(message))
                message = {'method': 'delete_ranges',
                           'ranges': old_hosts_ranges[host]}
                requests.patch(f'http://{replica}/', json.dumps(message))
        for host in old_nodes:
            for new_host in new_nodes:
                if new_host != host:
                    message = {'method': 'add_ranges_to_host',
                               'host': new_host,
                               'ranges': new_hosts_ranges[new_host]}
                    requests.patch(f'http://{host}/', json.dumps(message))
                    message = {'method': 'delete_ranges',
                               'ranges': new_hosts_ranges[new_host]}
                    requests.patch(f'http://{host}/', json.dumps(message))
        for host in new_nodes:
            for replica in new_hosts_replicas[host]:
                message = {'method': 'add_ranges_to_host',
                           'host': replica,
                           'ranges': new_hosts_ranges[host]}
                requests.patch(f'http://{host}/', json.dumps(message))
        self.hash_ring = new_ring
        return web.Response(status=200)
