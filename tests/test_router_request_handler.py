from client.__main__ import get_value, insert_value, delete_value
from tests.conftest import ROUTER_PORT, DATABASES_PORTS


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
