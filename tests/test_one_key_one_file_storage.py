from pathlib import Path

import pytest
from kvstorage.one_key_one_file_storage import OneKeyOneFileStorage


def create_one_key_one_file_storage_in_current_directory():
    storage = OneKeyOneFileStorage(Path('.'))
    return storage


def test_create_file_if_key_has_not_been_in_storage(chdir_to_temp_directory):
    storage = create_one_key_one_file_storage_in_current_directory()
    assert not Path('./apple').exists()
    storage.insert('apple', 'banana')
    assert Path('./apple').exists()
