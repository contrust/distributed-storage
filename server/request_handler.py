import aiohttp
from aiohttp import web
from kvstorage.one_key_one_file_storage import KVStorage


class RequestHandler:
    def __init__(self, databases: dict[str, KVStorage]):
        self.databases = databases

    async def handle_get_request(self, request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        if key and dbname in self.databases:
            value = self.databases[dbname].get(key)
            return web.Response(text=value)
        return web.Response(status=404)

    async def handle_post_request(self, request: aiohttp.request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        if key and dbname in self.databases:
            self.databases[dbname].insert(key, await request.post())

    async def handle_delete_request(self, request):
        dbname = request.match_info.get('dbname')
        key = request.match_info.get('key')
        if key and dbname in self.databases:
            self.databases[dbname].delete(key)
