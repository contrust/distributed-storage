import argparse

from client.client import get_dns_server_response


def parse_arguments():
    """
    Parse console arguments.
    """
    parser = argparse.ArgumentParser(
        prog=None if not globals().get('__spec__')
        else f'python3 -m {__spec__.name.partition(".")[0]}'
    )
    parser.add_argument('-dbname', '--database-name',
                        metavar='name',
                        required=True,
                        help='domain to get public ip')
    parser.add_argument('-s', '--server',
                        metavar='hostname',
                        required=False,
                        help='dns server hostname, localhost by default')
    parser.add_argument('-p', '--port',
                        metavar='port',
                        required=False,
                        help='port of dns server, 2021 by default')
    return parser.parse_args()


def main():
    args_dict = vars(parse_arguments())
    name = args_dict['domain']
    server = args_dict['server'] if args_dict['server'] else 'localhost'
    port = int(args_dict['port']) if args_dict['port'] else 2021
    server_response = get_dns_server_response(server, port, port, udp,)
    print(server_response)


if __name__ == '__main__':
    main()
