from abc import ABC, abstractmethod


class KVStorage(ABC):

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def insert(self, key, value):
        pass

    @abstractmethod
    def delete(self, key):
        pass
