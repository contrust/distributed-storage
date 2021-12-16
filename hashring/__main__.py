#!/usr/bin/env python3
import argparse
import pickle
import sys

from hashring.hashring import HashRing


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    parser.add_argument('OUTPUT_FILE',
                        type=str)
    methods = parser.add_mutually_exclusive_group(required=True)
    methods.add_argument('-c', '--create',
                         action='store_true',
                         help='create new hash ring.')
    methods.add_argument('-g', '--get',
                         metavar='INPUT_FILE',
                         help='read hash ring from hash ring pickle file.')
    actions = parser.add_mutually_exclusive_group(required=False)
    actions.add_argument('-a', '--add',
                         metavar='NODE_NAME',
                         help='add node to hash ring.')
    actions.add_argument('-r', '--remove',
                         metavar='NODE_NAME',
                         help='remove node from hash ring.')
    parser.add_argument('-n', '--nodes',
                        action='store_true',
                        help='show node names in resulting hash ring.')
    return parser.parse_args()


def main():
    args_dict = vars(parse_arguments())
    output_file_name = args_dict['OUTPUT_FILE']
    if args_dict['get']:
        with open(args_dict['get'], 'rb') as inp:
            ring = pickle.load(inp)
    else:
        ring = HashRing([], 100, 1)
    if args_dict['add']:
        try:
            ring.add_node(args_dict['add'])
        except ValueError:
            print('Can not add node because it is already in ring.')
            sys.exit()
    elif args_dict['remove']:
        try:
            ring.remove_node(args_dict['remove'])
        except ValueError:
            print('Can not remove node because it is not in ring.')
            sys.exit()
    if args_dict['nodes']:
        for node in ring.nodes:
            print(node)
    with open(output_file_name, 'wb') as out:
        pickle.dump(ring, out, pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    main()
