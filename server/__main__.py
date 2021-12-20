import argparse
import json
import pickle
import sys
from pathlib import Path
from sys import argv

import requests
from aiohttp import web

from database.four_parts_hash_storage import FourPartsHashStorage
from hashring.__main__ import load_ring, unload_ring
from server.database_config import DatabaseConfig
from server.database_request_handler import DatabaseRequestHandler
from server.request_handler import RequestHandler
from server.router_config import RouterConfig
from server.router_request_handler import RouterRequestHandler


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    methods = parser.add_mutually_exclusive_group(
        required=True)
    methods.add_argument('-u', '--update',
                         metavar=tuple(['host', 'hash_ring_file']),
                         nargs=2,
                         help='update router\'s hash ring')
    methods.add_argument('-r', '--run',
                         metavar='input_file',
                         help='run server with config')
    methods.add_argument('-g', '--get-config',
                         metavar='output_file',
                         help='get config file')
    server_type = parser.add_mutually_exclusive_group(
        required=('-u' not in argv and '--update' not in argv))
    server_type.add_argument('-db', '--database',
                             action='store_true')
    server_type.add_argument('-rt', '--router',
                             action='store_true')
    return parser.parse_args()


def run_server(handler: RequestHandler, hostname: str, port: int):
    app = web.Application()
    app.add_routes([web.get('/{dbname}/{key}', handler.handle_get_request),
                    web.post('/{dbname}/{key}', handler.handle_post_request),
                    web.delete('/{dbname}/{key}',
                               handler.handle_delete_request),
                    web.patch('/', handler.handle_patch_request)])
    web.run_app(app, host=hostname, port=port)


def main():
    args_dict = vars(parse_arguments())
    config = DatabaseConfig() if args_dict['database'] else RouterConfig()
    if args_dict['update']:
        host = args_dict['update'][0]
        input_file = args_dict['update'][1]
        ring = load_ring(input_file)
        message = ring.__dict__
        requests.patch(f'http://{host}/', json.dumps(message))
        sys.exit()
    if args_dict['get_config']:
        try:
            if args_dict['database']:
                config.databases = {'dbname': 'db/path'}
            else:
                config.hash_ring_path = "hashring.pkl"
            config.hostname = 'localhost'
            config.port = 2020
            config.unload(args_dict['get_config'])
        finally:
            sys.exit()
    config.load(args_dict['run'])
    if args_dict['database']:
        try:
            databases = {name: FourPartsHashStorage(Path(directory))
                         for name, directory in config.databases.items()}
        except ValueError as e:
            print(e)
            sys.exit()
        handler = DatabaseRequestHandler(databases)
    else:
        with open(config.hash_ring_path, 'rb') as inp:
            ring = pickle.load(inp)
        handler = RouterRequestHandler(ring)
    try:
        run_server(handler=handler, hostname=config.hostname, port=config.port)
    finally:
        if args_dict['router']:
            unload_ring(handler.hash_ring, config.hash_ring_path)


if __name__ == '__main__':
    main()
