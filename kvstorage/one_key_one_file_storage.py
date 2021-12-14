from pathlib import Path
from gzip import compress, decompress

from kvstorage.key_value_storage import KVStorage


class OneKeyOneFileStorage(KVStorage):
    def __init__(self, directory: Path):
        self.directory = directory

    def get(self, key):
        try:
            bytes_value = (self.directory / key).read_bytes()
            decompressed_bytes_value = decompress(bytes_value)
            value = decompressed_bytes_value.decode('utf-8')
        except FileNotFoundError:
            return None
        return value

    def insert(self, key, value):
        try:
            data = compress(value.encode('utf-8'))
            (self.directory / key).write_bytes(data)
        except FileNotFoundError:
            pass

    def delete(self, key):
        try:
            (self.directory / key).unlink()
        except FileNotFoundError:
            pass

    def traverse_keys(self):
        for path in self.directory.iterdir():
            if path.is_file():
                yield path.name
