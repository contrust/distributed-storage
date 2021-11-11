from pathlib import Path

from kvstorage.key_value_storage import KVStorage


class OneKeyOneFileStorage(KVStorage):
    def __init__(self, directory: Path):
        self.directory = directory

    def get(self, key):
        value = (self.directory / key).read_text('utf-8')
        return value

    def insert(self, key, value):
        (self.directory / key).write_text(value)

    def delete(self, key):
        (self.directory / key).unlink()
