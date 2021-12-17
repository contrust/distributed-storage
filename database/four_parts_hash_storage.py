import json
from pathlib import Path

from database.key_value_storage import KVStorage
from hashring.hashring import hash_to_32bit_int


def get_four_parts_of_hash(key: int):
    parts = []
    for i in range(4):
        parts.append(key % 256)
        key //= 256
    parts.reverse()
    return parts


def get_hex_hash_parts_from_key(key: str):
    key_bytes = key.encode('utf-8')
    key_hash = hash_to_32bit_int(key_bytes)
    hash_parts = get_four_parts_of_hash(key_hash)
    return list(map(lambda x: hex(x)[2:], hash_parts))


def get_key_path(start_directory: Path, key: str):
    hash_parts = get_hex_hash_parts_from_key(key)
    key_path = (start_directory /
                hash_parts[0] /
                hash_parts[1] /
                hash_parts[2] /
                hash_parts[3])
    return key_path


def is_dir_empty(directory: Path):
    return not any(Path(directory).iterdir())


class FourPartsHashStorage(KVStorage):
    def __init__(self, directory: Path):
        self.directory = directory

    def get(self, key):
        try:
            key_path = get_key_path(self.directory, key)
            if key_path.exists() and key_path.is_file():
                with open(key_path) as json_file:
                    values = json.load(json_file)
                    if key in values:
                        return values[key]
        except FileNotFoundError:
            return None
        return None

    def insert(self, key, value):
        try:
            hash_parts = get_hex_hash_parts_from_key(key)
            key_path = self.directory
            for i in range(3):
                if not (key_path / hash_parts[i]).exists():
                    (key_path / hash_parts[i]).mkdir()
                key_path = (key_path / hash_parts[i])
            key_path = get_key_path(self.directory, key)
            if key_path.exists() and key_path.is_file():
                with open(key_path) as json_file:
                    values = json.load(json_file)
            else:
                values = {}
            values[key] = value
            with open(key_path, mode='w') as file:
                json.dump(values, file, indent=4)
        except FileNotFoundError:
            pass

    def delete(self, key):
        try:
            key_path = get_key_path(self.directory, key)
            if key_path.exists() and key_path.is_file():
                with open(key_path) as json_file:
                    values = json.load(json_file)
            else:
                values = {}
            if key in values:
                del values[key]
            with open(key_path, mode='w') as file:
                json.dump(values, file, indent=4)
            if not values:
                key_path.unlink()
                for _ in range(3):
                    if is_dir_empty(key_path.parent):
                        key_path.parent.rmdir()
                    key_path = key_path.parent
        except FileNotFoundError:
            pass

    def traverse_keys(self):
        for path in self.directory.glob('*/*/*/*'):
            if path.is_file():
                with open(path) as json_file:
                    values = json.load(json_file)
                    for value in values:
                        yield value
