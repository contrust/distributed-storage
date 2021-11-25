from pathlib import Path

from kvstorage.key_value_storage import KVStorage


class OneKeyOneFileStorage(KVStorage):
    def __init__(self, directory: Path):
        self.directory = directory

    def get(self, key):
        try:
            value = (self.directory / key).read_text('utf-8')
        except FileNotFoundError:
            return None
        return value

    def insert(self, key, value):
        try:
            (self.directory / key).write_text(value)
        except FileNotFoundError:
            pass

    def delete(self, key):
        try:
            (self.directory / key).unlink()
        except FileNotFoundError:
            pass
