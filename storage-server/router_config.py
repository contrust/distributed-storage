from server.config import Config


class RouterConfig(Config):
    def __init__(self, hostname: str = None, port: int = None,
                 servers: list[str] = None):
        super().__init__(hostname, port)
        self.servers = servers if servers else []
