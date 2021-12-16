import aiohttp
import requests
from aiohttp import web

from consistent_hashing.hash_ring import HashRing, hash_to_32bit_int
from kvstorage.one_key_one_file_storage import KVStorage
from storage_server.request_handler import RequestHandler


class DatabaseRequestHandler(RequestHandler):
    def __init__(self, databases: dict[str, KVStorage]):
        self.databases = databases

    async def handle_get_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        if key and dbname in self.databases:
            value = self.databases[dbname].get(key)
            if value:
                return web.Response(text=value)
        return web.Response(status=404)

    async def handle_post_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        if key and dbname in self.databases:
            self.databases[dbname].insert(key, await request.text())
            return web.Response(status=200)
        return web.Response(status=400)

    async def handle_delete_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        if key and dbname in self.databases:
            self.databases[dbname].delete(key)
            return web.Response(status=200)
        return web.Response(status=400)

    async def handle_patch_request(self, request: aiohttp.request):
        message = await request.json()
        method = message['method']
        if method == 'delete_ranges_from_nodes':
            nodes = message['nodes']
            ranges = message['ranges']
            for dbname, database in self.databases.items():
                for key in database.traverse_keys():
                    key_hash = HashRing.hash_to_32bit_int(key.encode('utf-8'))
                    for tuple_range in ranges:
                        start = tuple_range[0]
                        end = tuple_range[1]
                        if start <= key_hash < end:
                            for node in nodes:
                                url = f'http://{node}/{dbname}/{key}'
                                try:
                                    requests.delete(url)
                                except:
                                    pass
        elif method == 'migrate_ranges_to_nodes':
            migration = message['migration']
            zones = message['zones']
            for dbname, database in self.databases.items():
                for key in database.traverse_keys():
                    key_hash = HashRing.hash_to_32bit_int(key.encode('utf-8'))
                    key_in_zone = False
                    for tuple_range in zones:
                        start = tuple_range[0]
                        end = tuple_range[1]
                        if start <= key_hash < end:
                            key_in_zone = True
                            break
                    if not key_in_zone:
                        continue
                    for hostname in migration:
                        if hostname != request.host:
                            for tuple_range in migration[hostname]:
                                start = tuple_range[0]
                                end = tuple_range[1]
                                if start <= key_hash < end:
                                    value = database.get(key)
                                    url = f'http://{hostname}/{dbname}/{key}'
                                    try:
                                        requests.post(url, data=value.encode(
                                            'utf-8'))
                                    except:
                                        pass
            for dbname, database in self.databases.items():
                for key in database.traverse_keys():
                    key_hash = hash_to_32bit_int(
                        key.encode('utf-8'))
                    key_in_zone = False
                    for tuple_range in zones:
                        start = tuple_range[0]
                        end = tuple_range[1]
                        if start <= key_hash < end:
                            key_in_zone = True
                            break
                    if not key_in_zone:
                        continue
                    for hostname in migration:
                        if hostname != request.host:
                            for tuple_range in migration[hostname]:
                                start = tuple_range[0]
                                end = tuple_range[1]
                                if start <= key_hash < end:
                                    database.delete(key)
        elif method == 'add_ranges_to_nodes':
            nodes = message['nodes']
            ranges = message['ranges']
            for dbname, database in self.databases.items():
                for key in database.traverse_keys():
                    key_hash = hash_to_32bit_int(key.encode('utf-8'))
                    for tuple_range in ranges:
                        start = tuple_range[0]
                        end = tuple_range[1]
                        if start <= key_hash < end:
                            for node in nodes:
                                value = database.get(key)
                                url = f'http://{node}/{dbname}/{key}'
                                try:
                                    requests.post(url,
                                                  data=value.encode('utf-8'))
                                except:
                                    pass
        else:
            web.Response(status=405)
        return web.Response(status=200)
