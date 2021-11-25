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
                        help='hostname of storage server')
    parser.add_argument('-P', '--port',
                        metavar='port',
                        help='port of storage server')
    parser.add_argument('-D', '--database',
                        metavar='dbname',
                        help='database name')
    methods = parser.add_mutually_exclusive_group()
    methods.add_argument('-GET',
                         action='store_true',
                         help='get value from database')
    methods.add_argument('-INSERT',
                         action='store_true',
                         help='insert value in database')
    methods.add_argument('-DELETE',
                         action='store_true',
                         help='delete key in database')
    parser.add_argument('-K', '--key',
                        metavar='key')
    parser.add_argument('-V', '--value',
                        metavar='value')
    return parser.parse_args()


def main():
    args = vars(parse_arguments())
    url = f'http://{args["hostname"]}:{args["port"]}/' \
          f'{args["database"]}/{args["key"]}'
    if args['GET']:
        response = requests.get(url)
        print(response.text)
    elif args['INSERT']:
        requests.post(url, data=args["value"].encode('utf-8'))
    else:
        requests.delete(url)


if __name__ == '__main__':
    main()
