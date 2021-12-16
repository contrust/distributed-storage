import argparse
import json
import pickle
import sys
from pathlib import Path
from sys import argv

import requests
from aiohttp import web

from server.request_handler import RequestHandler
from database.one_key_one_file_storage import OneKeyOneFileStorage
from server.database_config import DatabaseConfig
from server.database_request_handler import DatabaseRequestHandler
from server.router_config import RouterConfig
from server.router_request_handler import RouterRequestHandler


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    parser.add_argument('-u',
                        nargs=2,
                        metavar=(
                            'OLD_HASH_RING_FILE', 'NEW_HASH_RING_FILE')),
    server_type = parser.add_mutually_exclusive_group(
        required=('-u' not in argv))
    server_type.add_argument('-db', '--database',
                             action='store_true',
                             help='define database server.')
    server_type.add_argument('-rt', '--router',
                             action='store_true',
                             help='define router server.')
    actions = parser.add_mutually_exclusive_group(
        required=('-db' in argv or '-rt' in argv))
    actions.add_argument('-r', '--run',
                         metavar='CONFIG_FILE',
                         required=False,
                         help='run server with given config.')
    actions.add_argument('-g', '--get-config',
                         metavar='OUTPUT_FILE',
                         required=False,
                         help='get config file in given path.')
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
    if args_dict['u']:
        with open(args_dict['u'][0], 'rb') as inp:
            old_ring = pickle.load(inp)
        with open(args_dict['u'][1], 'rb') as inp:
            new_ring = pickle.load(inp)
        old_nodes = list(old_ring.nodes.keys())
        new_nodes = list(new_ring.nodes.keys())
        old_hosts_ranges = old_ring.get_nodes_ranges()
        old_hosts_replicas = old_ring.nodes_replicas
        new_hosts_ranges = new_ring.get_nodes_ranges()
        new_hosts_replicas = new_ring.nodes_replicas
        for host in old_nodes:
            host_ranges = old_hosts_ranges[host]
            host_replicas = old_hosts_replicas[host]
            message = {'method': 'delete_ranges_from_nodes',
                       'nodes': host_replicas,
                       'ranges': host_ranges}
            requests.patch(f'http://{host}/', json.dumps(message))
        for host in old_nodes:
            message = {'method': 'migrate_ranges_to_nodes',
                       'zones': old_hosts_ranges[host],
                       'migration': new_hosts_ranges}
            requests.patch(f'http://{host}/', json.dumps(message))
        for host in new_nodes:
            host_ranges = new_hosts_ranges[host]
            host_replicas = new_hosts_replicas[host]
            message = {'method': 'add_ranges_to_nodes',
                       'nodes': host_replicas,
                       'ranges': host_ranges}
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
        databases = {name: OneKeyOneFileStorage(Path(directory))
                     for name, directory in config.databases.items()}
        handler = DatabaseRequestHandler(databases)
    else:
        with open(config.hash_ring_path, 'rb') as inp:
            ring = pickle.load(inp)
        handler = RouterRequestHandler(ring)
    run_server(handler=handler, hostname=config.hostname, port=config.port)


if __name__ == '__main__':
    main()
