from client.__main__ import get_value, insert_value, delete_value
from hashring.ring import hash_to_32bit_int
from server.router_request_handler import delete_ranges_from_host, \
    add_host_ranges_to_another_host
from tests.conftest import ROUTER_PORT, DATABASES_PORTS


def get_sorted_keys_hashes(keys: iter) -> dict:
    keys_hashes = {}
    for key in keys:
        encoded_key = key.encode('utf-8')
        keys_hash = hash_to_32bit_int(encoded_key)
        if keys_hash not in keys_hashes:
            keys_hashes[keys_hash] = set()
        keys_hashes[keys_hash].add(key)
    return {x: y for x, y in sorted(keys_hashes.items())}


def test_key_in_two_databases_after_insert():
    insert_value('localhost', ROUTER_PORT, 'panama', 'key', 'vov')
    databases_values = []
    for database_port in DATABASES_PORTS:
        value = get_value('localhost', database_port, 'panama', 'key')
        databases_values.append(value)
    assert databases_values.count('vov') == 2


def test_there_is_no_key_in_any_database_after_delete():
    insert_value('localhost', ROUTER_PORT, 'panama', 'key', 'vov')
    delete_value('localhost', ROUTER_PORT, 'panama', 'key')
    databases_values = []
    for database_port in DATABASES_PORTS:
        value = get_value('localhost', database_port, 'panama', 'key')
        databases_values.append(value)
    assert databases_values.count('vov') == 0


def test_get_inserted_value_even_when_key_is_not_found_in_one_database():
    for database_port in DATABASES_PORTS:
        insert_value('localhost', ROUTER_PORT, 'panama', 'pringles', 'pack')
        delete_value('localhost', database_port, 'panama', 'key')
        value = get_value('localhost', ROUTER_PORT, 'panama', 'pringles')
        assert value == 'pack'


def test_keys_within_ranges_are_deleted_after_delete_ranges_from_host():
    keys_hashes = get_sorted_keys_hashes(map(str, range(10)))
    hashes = list(keys_hashes.keys())
    for key in map(str, range(10)):
        insert_value('localhost', DATABASES_PORTS[0], 'panama', key, 'ok')
    range_without_first_hash = [hashes[1], 2 ** 32]
    delete_ranges_from_host(f'localhost:{DATABASES_PORTS[0]}',
                            [range_without_first_hash])
    existing_keys = set(key for key in map(str, range(10)) if
                        get_value('localhost', DATABASES_PORTS[0], 'panama',
                                  key) != '')
    assert existing_keys == keys_hashes[hashes[0]]


def test_keys_within_ranges_are_added_after_add_ranges_from_host():
    two_hash = hash_to_32bit_int(b'2')
    for key in '1', '2', '3':
        insert_value('localhost', DATABASES_PORTS[0], 'panama', key,
                     'some_value')
        value = get_value('localhost', DATABASES_PORTS[0], 'panama', key)
        assert value == 'some_value'
    for key in '1', '2', '3':
        value = get_value('localhost', DATABASES_PORTS[1], 'panama', key)
        assert value == ''
    add_host_ranges_to_another_host(f'localhost:{DATABASES_PORTS[0]}',
                                    f'localhost:{DATABASES_PORTS[1]}',
                                    [[two_hash, two_hash + 1]])
    for key in '1', '2', '3':
        value = get_value('localhost', DATABASES_PORTS[1], 'panama', key)
        if value:
            assert key == '2'
        else:
            assert key in {'1', '3'}
