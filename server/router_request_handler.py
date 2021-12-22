import json

import aiohttp
import requests
from aiohttp import web, ClientConnectionError

from hashring.hashring import HashRing
from server.request_handler import RequestHandler


def add_host_ranges_to_another_host(host: str, another_host: str,
                                    ranges: list) -> None:
    message = {'method': 'add_ranges_to_host',
               'host': another_host,
               'ranges': ranges}
    requests.patch(f'http://{host}/', json.dumps(message))


def delete_ranges_from_host(host: str, ranges: list):
    message = {'method': 'delete_ranges',
               'ranges': ranges}
    requests.patch(f'http://{host}/', json.dumps(message))


def delete_hosts_ranges_from_its_replicas(ring: HashRing) -> None:
    hosts_ranges = ring.get_nodes_ranges()
    for host in ring.nodes_replicas:
        for replica in ring.nodes_replicas[host]:
            try:
                add_host_ranges_to_another_host(replica, host,
                                                hosts_ranges[host])
                delete_ranges_from_host(replica, hosts_ranges[host])
            except requests.exceptions.ConnectionError:
                print(f'Can not connect to {replica}')
                continue


def migrate_old_nodes_ranges_to_new_nodes_ranges(old_ring: HashRing,
                                                 new_ring: HashRing) -> None:
    new_hosts_ranges = new_ring.get_nodes_ranges()
    for host in old_ring.nodes:
        for new_host in new_ring.nodes:
            if new_host != host:
                try:
                    add_host_ranges_to_another_host(host, new_host,
                                                    new_hosts_ranges[new_host])
                    delete_ranges_from_host(host, new_hosts_ranges[new_host])
                except requests.exceptions.ConnectionError:
                    print(f'Can not connect to {host}')
                    continue


def add_hosts_ranges_to_its_replicas(ring: HashRing) -> None:
    hosts_ranges = ring.get_nodes_ranges()
    for host in ring.nodes_replicas:
        for replica in ring.nodes_replicas[host]:
            try:
                add_host_ranges_to_another_host(host, replica,
                                                hosts_ranges[host])
            except requests.exceptions.ConnectionError:
                print(f'Can not connect to {host}')
                continue


class RouterRequestHandler(RequestHandler):
    def __init__(self, hash_ring: HashRing):
        self.hash_ring = hash_ring
        self.nodes = hash_ring.nodes

    async def handle_get_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_string(key)
        async with aiohttp.ClientSession() as session:
            for replica in [node] + self.hash_ring.nodes_replicas[node]:
                try:
                    async with session.get(
                            f'http://{replica}/{dbname}/{key}') as resp:
                        value = await resp.text()
                        if value:
                            return web.Response(text=value)
                except ClientConnectionError:
                    continue
        return web.Response(status=404)

    async def handle_post_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_string(key)
        responses = []
        async with aiohttp.ClientSession() as session:
            for replica in [node] + self.hash_ring.nodes_replicas[node]:
                try:
                    async with session.post(f'http://{replica}/{dbname}/{key}',
                                            data=await request.read()) as resp:
                        responses.append(resp.status)
                except ClientConnectionError:
                    responses.append(400)
        return web.Response(status=(200 if 200 in responses else 400))

    async def handle_delete_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        node = self.hash_ring.find_node_for_string(key)
        responses = []
        async with aiohttp.ClientSession() as session:
            for replica in [node] + self.hash_ring.nodes_replicas[node]:
                try:
                    async with session.delete(
                            f'http://{replica}/{dbname}/{key}') as resp:
                        responses.append(resp.status)
                except ClientConnectionError:
                    responses.append(400)
        return web.Response(status=(200 if 200 in responses else 400))

    async def handle_patch_request(self, request: aiohttp.request):
        message = await request.json()
        message['keys'] = {int(x): y for x, y in message['keys'].items()}
        new_ring = HashRing()
        new_ring.__dict__.update(message)
        old_ring = self.hash_ring
        delete_hosts_ranges_from_its_replicas(old_ring)
        migrate_old_nodes_ranges_to_new_nodes_ranges(old_ring, new_ring)
        add_hosts_ranges_to_its_replicas(new_ring)
        self.hash_ring = new_ring
        return web.Response(status=200)
