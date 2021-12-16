from server.config import Config


class RouterConfig(Config):
    def __init__(self, hash_ring_path: str = None,
                 hostname: str = None, port: int = None):
        super().__init__(hostname, port)
        self.hash_ring_path = hash_ring_path
