from pathlib import Path
from kvstorage.one_key_one_file_storage import OneKeyOneFileStorage


def create_one_key_one_file_storage_in_current_directory():
    storage = OneKeyOneFileStorage(Path('.'))
    return storage


def test_create_file_if_key_has_not_been_in_storage(chdir_to_temp_directory):
    storage = create_one_key_one_file_storage_in_current_directory()
    assert not Path('./apple').exists()
    storage.insert('apple', 'banana')
    assert Path('./apple').exists()


def test_created_file_has_the_same_value(chdir_to_temp_directory):
    storage = create_one_key_one_file_storage_in_current_directory()
    storage.insert('apple', 'banana')
    content = Path('./apple').read_text()
    assert content == 'banana'


def test_file_not_exist_if_delete_key(chdir_to_temp_directory):
    storage = create_one_key_one_file_storage_in_current_directory()
    storage.insert('apple', 'banana')
    storage.delete('apple')
    assert not Path('./apple').exists()


def test_get_the_same_inserted_value(chdir_to_temp_directory):
    storage = create_one_key_one_file_storage_in_current_directory()
    storage.insert('apple', 'banana')
    value = storage.get('apple')
    assert value == 'banana'
