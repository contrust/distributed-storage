from pathlib import Path
from database.four_parts_hash_storage import FourPartsHashStorage, get_key_path, \
    get_values_by_key_path


def create_four_parts_hash_storage_in_current_directory():
    storage = FourPartsHashStorage(Path('.'))
    return storage


def test_create_file_if_key_has_not_been_in_storage(chdir_to_temp_directory):
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'koala'
    key_path = get_key_path(Path('.'), key)
    assert not key_path.exists()
    storage.insert(key, 'apple')
    assert key_path.exists()


def test_created_file_has_the_same_value(chdir_to_temp_directory):
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'koala'
    value = 'apple'
    storage.insert(key, value)
    key_path = get_key_path(Path('.'), key)
    values = get_values_by_key_path(key_path)
    assert key in values
    assert values[key] == value


def test_file_not_exist_if_delete_key_in_file_with_one_key(
        chdir_to_temp_directory):
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'koala'
    key_path = get_key_path(Path('.'), key)
    storage.insert(key, 'banana')
    assert key_path.exists()
    storage.delete(key)
    assert not key_path.exists()


def test_get_the_same_inserted_value(chdir_to_temp_directory):
    storage = create_four_parts_hash_storage_in_current_directory()
    key = 'mandarin'
    inserted_value = 'happy new year'
    storage.insert(key, inserted_value)
    value = storage.get(key)
    assert inserted_value == value
