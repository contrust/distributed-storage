import aiohttp
import requests
from aiohttp import web

from hashring.hashring import HashRing, hash_to_32bit_int
from database.one_key_one_file_storage import KVStorage
from server.request_handler import RequestHandler


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
        if method == 'delete_ranges':
            ranges = message['ranges']
            for dbname, database in self.databases.items():
                for key in database.traverse_keys():
                    key_hash = hash_to_32bit_int(key.encode('utf-8'))
                    for tuple_range in ranges:
                        start = int(tuple_range[0])
                        end = int(tuple_range[1])
                        if start <= key_hash < end:
                            database.delete(key)
        elif method == 'add_ranges_to_host':
            host = message['host']
            ranges = message['ranges']
            for dbname, database in self.databases.items():
                for key in database.traverse_keys():
                    key_hash = hash_to_32bit_int(key.encode('utf-8'))
                    for tuple_range in ranges:
                        start = int(tuple_range[0])
                        end = int(tuple_range[1])
                        if start <= key_hash < end:
                            value = database.get(key)
                            url = f'http://{host}/{dbname}/{key}'
                            try:
                                requests.post(url,
                                              data=value.encode('utf-8'))
                            except:
                                pass
        else:
            web.Response(status=405)
        return web.Response(status=200)
