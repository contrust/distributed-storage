#!/usr/bin/env python3
import argparse
import pickle
import sys
from pathlib import Path

from aiohttp import web

from kvstorage.one_key_one_file_storage import OneKeyOneFileStorage
from storage_server.database_config import DatabaseConfig
from storage_server.database_request_handler import DatabaseRequestHandler
from storage_server.router_config import RouterConfig
from storage_server.router_request_handler import RouterRequestHandler


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    parser.add_argument('-u', '--update',
                        metavar='NEW_HASH_RING_FILE_PATH',
                        help='update hash ring.')
    server_type = parser.add_mutually_exclusive_group(required=True)
    server_type.add_argument('-db', '--database',
                             action='store_true',
                             help='define database server.')
    server_type.add_argument('-rt', '--router',
                             action='store_true',
                             help='define router server.')
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument('-r', '--run',
                         metavar='CONFIG_FILE',
                         required=False,
                         help='run storage_server with given config.')
    actions.add_argument('-g', '--get-config',
                         metavar='OUTPUT_FILE',
                         required=False,
                         help='get config file in given path.')
    return parser.parse_args()


def main():
    args_dict = vars(parse_arguments())
    config = DatabaseConfig() if args_dict['database'] else RouterConfig()
    if args_dict['get_config']:
        try:
            if args_dict['database']:
                config.databases = {'dbname': 'db/path'}
            else:
                config.hash_ring_path = "hash_ring.pkl"
            config.hostname = 'localhost'
            config.port = 2020
            config.unload(args_dict['get_config'])
        finally:
            sys.exit()
    config.load(args_dict['run'])
    if args_dict['database']:
        databases = {name: OneKeyOneFileStorage(Path(directory))
                     for name, directory in config.databases.items()}
        handler = DatabaseRequestHandler(databases)
    else:
        with open(config.hash_ring_path, 'rb') as inp:
            ring = pickle.load(inp)
        handler = RouterRequestHandler(ring)
    app = web.Application()
    routes = [web.get('/{dbname}/{key}', handler.handle_get_request),
              web.post('/{dbname}/{key}', handler.handle_post_request),
              web.delete('/{dbname}/{key}', handler.handle_delete_request)]
    if args_dict['database']:
        routes.append(web.patch('/', handler.handle_patch_request))
    app.add_routes(routes)
    web.run_app(app, host=config.hostname, port=config.port)


if __name__ == '__main__':
    main()
