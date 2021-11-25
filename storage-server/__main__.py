#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from server.config import Config
from server.database_config import DatabaseConfig
from server.database_request_handler import DatabaseRequestHandler
from aiohttp import web
from kvstorage.one_key_one_file_storage import OneKeyOneFileStorage
from server.router_config import RouterConfig
from server.router_request_handler import RouterRequestHandler


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    server_type = parser.add_mutually_exclusive_group(required=True)
    server_type.add_argument('-db', '--database',
                        action='store_true',
                        help='define database server')
    server_type.add_argument('-rt', '--router',
                        action='store_true',
                        help='define router server')
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument('-r', '--run',
                        metavar='CONFIG_FILE',
                        required=False,
                        help='run storage-server with given config')
    actions.add_argument('-g', '--get-config',
                        metavar='OUTPUT_FILE',
                        required=False,
                        help='get config file in given path')
    return parser.parse_args()


def main():
    args_dict = vars(parse_arguments())
    config = DatabaseConfig() if args_dict['database'] else RouterConfig()
    if args_dict['get_config']:
        try:
            if args_dict['database']:
                config.databases = {'dbname': 'db/path'}
            else:
                config.servers = ['address']
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
        handler = RouterRequestHandler(config.servers)
    app = web.Application()
    app.add_routes([web.get('/{dbname}/{key}', handler.handle_get_request),
                   web.post('/{dbname}/{key}', handler.handle_post_request),
                   web.delete('/{dbname}/{key}', handler.handle_delete_request)])
    web.run_app(app, host=config.hostname, port=config.port)


if __name__ == '__main__':
    main()
