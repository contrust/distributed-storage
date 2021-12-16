#!/usr/bin/env python3
import argparse
import os
import sys
import requests


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    parser.add_argument('-H', '--hostname',
                        metavar='hostname',
                        required=True)
    parser.add_argument('-P', '--port',
                        metavar='port',
                        required=True)
    parser.add_argument('-D', '--database',
                        metavar='dbname',
                        required=True)
    methods = parser.add_mutually_exclusive_group(required=True)
    methods.add_argument('-G', '--get',
                         action='store_true',
                         help='get value from database')
    methods.add_argument('-A', '--insert',
                         action='store_true',
                         help='insert value in database')
    methods.add_argument('-R', '--delete',
                         action='store_true',
                         help='delete key in database')
    parser.add_argument('-K', '--key',
                        metavar='key',
                        required=True)
    parser.add_argument('-V', '--value',
                        metavar='value',
                        required=('-INSERT' in sys.argv))
    return parser.parse_args()


def get_url(hostname: str, port: int, database: str, key: str):
    return f'http://{hostname}:{port}/{database}/{key}'


def main(args):
    hostname = args["hostname"]
    port = args["port"]
    url = get_url(hostname, port, args["database"], args["key"])
    try:
        if args['get']:
            response = requests.get(url)
            print(response.text)
        elif args['insert']:
            requests.post(url, data=args["value"].encode('utf-8'))
        elif args['delete']:
            requests.delete(url)
    except requests.ConnectionError:
        print(f'Error: can not connect to {hostname}:{port}.')


if __name__ == '__main__':
    arguments = vars(parse_arguments())
    main(arguments)
