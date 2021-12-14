from kvstorage.key_value_storage import KVStorage
from storage_server.config import Config


class DatabaseConfig(Config):
    def __init__(self, hostname: str = None, port: int = None,
                 databases: dict[str, KVStorage] = None):
        super().__init__(hostname, port)
        self.databases = databases if databases else {}
