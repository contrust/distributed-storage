import client.__main__
from tests.conftest import DATABASES_PORTS


def get_value(database: str, key: str):
    value = client.__main__.get_value('localhost', DATABASES_PORTS[0], database,
                                      key)
    return value


def insert_value(database: str, key: str, value: str):
    client.__main__.insert_value('localhost', DATABASES_PORTS[0], database, key,
                                 value)


def delete_value(database: str, key: str):
    client.__main__.delete_value('localhost', DATABASES_PORTS[0], database, key)


def test_get_value_after_insert():
    insert_value('panama', 'koala', 'soup')
    value = get_value('panama', 'koala')
    assert value == 'soup'


def test_big_value_is_the_same_after_insert():
    value = 'a' * 1000000
    insert_value('panama', 'a', value)
    value2 = get_value('panama', 'a')
    assert value == value2


def test_big_key_is_the_same_after_insert():
    key = 'a' * 8000
    insert_value('panama', key, 'value')
    value = get_value('panama', key)
    assert value == 'value'


def test_value_changes_after_insert_with_different_value():
    insert_value('panama', 'koala', 'soup')
    insert_value('panama', 'koala', 'soda')
    value = get_value('panama', 'koala')
    assert value == 'soda'


def test_get_empty_string_after_delete():
    insert_value('panama', 'koala', 'soup')
    delete_value('panama', 'koala')
    value = get_value('panama', 'koala')
    assert value == ''


def test_key_exist_only_in_one_database_after_insert():
    insert_value('panama', 'koala', 'soup')
    panama_value = get_value('panama', 'koala')
    liana_value = get_value('liana', 'koala')
    assert panama_value == 'soup' and liana_value == ''


def test_key_is_deleted_only_in_one_database():
    insert_value('panama', 'liana', 'unicorn')
    insert_value('panama', 'koala', 'unicorn')
    delete_value('liana', 'koala')
    panama_value = get_value('panama', 'koala')
    liana_value = get_value('liana', 'koala')
    assert panama_value == 'unicorn' and liana_value == ''


def test_get_empty_string_if_try_get_not_existing_key():
    value = get_value('panama', 'akula')
    assert value == ''
