import json
from dataclasses import dataclass


@dataclass
class Config:
    def __init__(self, hostname: str = None, port: int = None):
        self.hostname = hostname
        self.port = port

    def load(self, path: str) -> None:
        with open(path) as json_file:
            data = json.load(json_file)
            self.__dict__.update(data)

    def unload(self, path: str) -> None:
        with open(path, mode='w') as file:
            json.dump(self.__dict__, file, indent=4)
