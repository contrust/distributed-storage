from aiohttp import web

from storage_server.request_handler import RequestHandler


def run_server(handler: RequestHandler, hostname: str, port: int):
    app = web.Application()
    app.add_routes([web.get('/{dbname}/{key}', handler.handle_get_request),
                    web.post('/{dbname}/{key}', handler.handle_post_request),
                    web.delete('/{dbname}/{key}',
                               handler.handle_delete_request),
                    web.patch('/', handler.handle_patch_request)])
    web.run_app(app, host=hostname, port=port)
