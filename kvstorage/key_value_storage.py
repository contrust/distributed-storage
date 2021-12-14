from abc import ABC, abstractmethod
from typing import Optional


class KVStorage(ABC):

    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        pass

    @abstractmethod
    def insert(self, key: str, value: str) -> None:
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        pass

    @abstractmethod
    def traverse_keys(self):
        pass
