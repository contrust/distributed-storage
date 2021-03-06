#!/usr/bin/env python3
import argparse
import pickle
import sys

from hashring.ring import HashRing


def parse_arguments(args):
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    methods = parser.add_mutually_exclusive_group(required=True)
    methods.add_argument('-c', '--create',
                         metavar=tuple(["number_of_keys_for_node",
                                        "number_of_replicas_for_node",
                                        "output_file"]),
                         nargs=3,
                         help='create new hash ring')
    methods.add_argument('-a', '--add-host',
                         metavar=tuple(["host",
                                        "input_file"]),
                         nargs=2,
                         help='add host to ring')
    methods.add_argument('-r', '--remove-host',
                         metavar=tuple(["host",
                                        "input_file"]),
                         nargs=2,
                         help='remove host from ring')
    methods.add_argument('-i', '--info',
                         metavar="input_file",
                         help='show ring info')
    return parser.parse_args(args)


def show_ring_info(ring: HashRing):
    print(f'Number of keys for node: {ring.keys_for_node_count}')
    for node in ring.nodes_replicas:
        print(f'Replicas of {node}: {" ".join(ring.nodes_replicas[node])}')


def load_ring(path: str):
    with open(path, 'rb') as inp:
        ring = pickle.load(inp)
    return ring


def unload_ring(ring: HashRing, path: str):
    with open(path, 'wb') as out:
        pickle.dump(ring, out, pickle.HIGHEST_PROTOCOL)


def main(args):
    args_dict = vars(parse_arguments(args))
    if args_dict['add_host']:
        host = args_dict['add_host'][0]
        input_file = args_dict['add_host'][1]
        ring = load_ring(input_file)
        ring.add_node(host)
        unload_ring(ring, input_file)
        print(f'Successfully added node {host}')
    elif args_dict['remove_host']:
        host = args_dict['remove_host'][0]
        input_file = args_dict['remove_host'][1]
        ring = load_ring(input_file)
        ring.remove_node(host)
        unload_ring(ring, input_file)
        print(f'Successfully removed node {host}')
    elif args_dict['create']:
        number_of_keys_for_node = int(args_dict['create'][0])
        number_of_replicas_for_node = int(args_dict['create'][1])
        output_file = args_dict['create'][2]
        ring = HashRing([], number_of_keys_for_node,
                        number_of_replicas_for_node)
        unload_ring(ring, output_file)
        print('Successfully created ring')
    elif args_dict['info']:
        input_file = args_dict['info']
        ring = load_ring(input_file)
        show_ring_info(ring)


def entry_point():
    main(sys.argv[1:])


if __name__ == '__main__':
    entry_point()
