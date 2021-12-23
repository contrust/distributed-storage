from pathlib import Path
from unittest.mock import patch

import pytest

from database.four_parts_hash_storage import FourPartsHashStorage, get_key_path, \
    get_values_by_key_path


def create_four_parts_hash_storage_in_current_directory():
    storage = FourPartsHashStorage(Path('.'))
    return storage


def test_create_file_if_key_has_not_been_in_storage():
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'koala'
    key_path = get_key_path(Path('.'), key)
    assert not key_path.exists()
    storage.insert(key, 'apple')
    assert key_path.exists()


def test_created_file_has_the_same_value():
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'koala'
    value = 'apple'
    storage.insert(key, value)
    key_path = get_key_path(Path('.'), key)
    values = get_values_by_key_path(key_path)
    assert key in values
    assert values[key] == value


def test_file_not_exists_if_delete_key_with_unique_hash():
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'koala'
    key_path = get_key_path(Path('.'), key)
    storage.insert(key, 'banana')
    assert key_path.exists()
    storage.delete(key)
    assert not key_path.exists()


@patch('database.four_parts_hash_storage.get_hex_hash_parts_from_key',
       lambda x: ['1'] * 4)
def test_file_exists_if_delete_key_with_not_unique_hash():
    storage = create_four_parts_hash_storage_in_current_directory()
    key1 = 'koala'
    key2 = 'cherry'
    key_path = get_key_path(Path('.'), key1)
    storage.insert(key1, 'banana')
    storage.insert(key2, 'banana')
    storage.delete(key1)
    assert key_path.exists()


def test_get_the_same_inserted_value():
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'mandarin'
    inserted_value = 'happy new year'
    storage.insert(key, inserted_value)
    value = storage.get(key)
    assert inserted_value == value


def test_traverse_all_inserted_keys_with_unique_hash():
    storage = create_four_parts_hash_storage_in_current_directory()
    keys = {str(i) for i in range(10)}
    value = 'happy new year'
    for key in keys:
        storage.insert(key, value)
    traversed_keys = set(storage.traverse_keys())
    assert traversed_keys == keys


@patch('database.four_parts_hash_storage.get_hex_hash_parts_from_key',
       lambda x: ['1'] * 4)
def test_traverse_all_inserted_keys_with_not_unique_hash():
    test_traverse_all_inserted_keys_with_unique_hash()


def test_return_none_if_get_not_inserted_key():
    storage = create_four_parts_hash_storage_in_current_directory()
    value = storage.get('hello')
    assert value is None


def test_exception_is_not_raised_if_delete_not_inserted_key():
    storage = create_four_parts_hash_storage_in_current_directory()
    storage.delete('hello')
    assert True
