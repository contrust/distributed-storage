import aiohttp
from abc import ABC, abstractmethod


class RequestHandler(ABC):
    @abstractmethod
    def handle_get_request(self, request: aiohttp.request):
        pass

    @abstractmethod
    def handle_post_request(self, request: aiohttp.request):
        pass

    @abstractmethod
    def handle_delete_request(self, request: aiohttp.request):
        pass
