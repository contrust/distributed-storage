from abc import ABC, abstractmethod


class KVStorage(ABC):

    @abstractmethod
    def get(self, key: str) -> str:
        pass

    @abstractmethod
    def insert(self, key: str, value: str) -> str:
        pass

    @abstractmethod
    def delete(self, key: str) -> str:
        pass
