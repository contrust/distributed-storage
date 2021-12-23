#!/usr/bin/env python3
import argparse
import sys
import requests


def parse_arguments(args):
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
    return parser.parse_args(args)


def get_url(hostname: str, port: int, database: str, key: str):
    return f'http://{hostname}:{port}/{database}/{key}'


def get_value(hostname: str, port: int, database: str, key: str):
    url = get_url(hostname, port, database, key)
    response = requests.get(url)
    return response.text


def insert_value(hostname: str, port: int, database: str, key: str, value: str):
    url = get_url(hostname, port, database, key)
    data = value.encode('utf-8')
    requests.post(url, data=data)


def delete_value(hostname: str, port: int, database: str, key: str):
    url = get_url(hostname, port, database, key)
    requests.delete(url)


def main(args):
    args_dict = vars(parse_arguments(args))
    hostname = args_dict["hostname"]
    port = args_dict["port"]
    db = args_dict['database']
    key = args_dict['key']
    try:
        if args_dict['get']:
            value = get_value(hostname, port, db, key)
            print(value)
        elif args_dict['insert']:
            value = args_dict['value']
            insert_value(hostname, port, db, key, value)
        elif args_dict['delete']:
            delete_value(hostname, port, db, key)
    except requests.ConnectionError:
        print(f'Error: can not connect to {hostname}:{port}.')


if __name__ == '__main__':
    main(sys.argv[1:])
