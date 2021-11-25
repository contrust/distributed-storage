#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

from server.config import Config
from server.request_handler import RequestHandler
from aiohttp import web
from kvstorage.one_key_one_file_storage import OneKeyOneFileStorage


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    parser.add_argument('-r', '--run',
                        metavar='CONFIG_FILE',
                        required=False,
                        help='run server with given config')
    parser.add_argument('-g', '--get-config',
                        metavar='OUTPUT_FILE',
                        required=False,
                        help='get config file in given path')
    return parser.parse_args()


def main():
    config = Config()
    args_dict = vars(parse_arguments())
    if args_dict['get_config']:
        try:
            config.unload(args_dict['get_config'])
        finally:
            sys.exit()
    if args_dict['run']:
        config.load(args_dict['run'])
    databases = {name: OneKeyOneFileStorage(Path(directory))
                 for name, directory in config.databases.items()}
    handler = RequestHandler(databases)
    app = web.Application()
    app.add_routes([web.get('/{dbname}/{key}', handler.handle_get_request),
                   web.post('/{dbname}/{key}', handler.handle_post_request),
                   web.delete('/{dbname}/{key}', handler.handle_delete_request)])
    web.run_app(app, host=config.hostname, port=config.port)


if __name__ == '__main__':
    main()
