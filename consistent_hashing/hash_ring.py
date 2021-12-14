import hashlib
import random


class HashRing:
    @staticmethod
    def hash_to_32bit_int(data: bytes):
        return int.from_bytes(hashlib.sha256(data).digest()[: 4], 'little')

    @staticmethod
    def get_keys_ranges_for_new_nodes_from_old_nodes(old_hash_ring,
                                                     new_hash_ring):
        keys_ranges = {}
        old_nodes = old_hash_ring.nodes
        new_added_nodes = new_hash_ring.nodes - old_hash_ring.nodes
        previous_key = None
        previous_old_key = None
        for ring_key, value in sorted(new_hash_ring.keys.items(), reverse=True):
            if previous_key is None:
                previous_key = ring_key
            if value in old_nodes and previous_old_key is None:
                previous_old_key = ring_key
                break
        if previous_old_key and previous_key:
            for ring_key, value in new_hash_ring.keys.items():
                if value in new_added_nodes:
                    previous_old_node = new_hash_ring.keys[previous_old_key]
                    if previous_old_node not in keys_ranges:
                        keys_ranges[previous_old_node] = {}
                    if value not in keys_ranges[previous_old_node]:
                        keys_ranges[previous_old_node][value] = []
                    keys_ranges[previous_old_node][value].append(
                        tuple([previous_key, ring_key]))
                previous_key = ring_key
                if value in old_nodes:
                    previous_old_key = ring_key
        return keys_ranges

    def __init__(self, nodes: iter, replicas: int = 5):
        self.replicas = replicas
        self.keys = {}
        self.nodes = set()
        for node in nodes:
            self.add_node(node)

    def find_node_for_bytes(self, data: str):
        data = data.encode('utf-8')
        key_hash = HashRing.hash_to_32bit_int(data)
        if self.keys:
            max_element = max(self.keys)
            last_element = None
            for data in self.keys:
                if key_hash < data:
                    break
                else:
                    print(data, key_hash)
                    last_element = data
            if last_element:
                node = self.keys[last_element]
            else:
                node = self.keys[max_element]
            return node
        else:
            raise ValueError

    def add_node(self, node: str):
        if node not in self.nodes:
            for _ in range(self.replicas):
                number = random.randint(1, 2 ** 32)
                if number not in self.keys:
                    self.keys[number] = node
            self.nodes.add(node)
            self.keys = {x: y for x, y in sorted(self.keys.items())}
        else:
            raise ValueError

    def remove_node(self, node: str):
        if node in self.nodes:
            for ring_key in list(self.keys.keys()):
                if self.keys[ring_key] == node:
                    del self.keys[ring_key]
            self.nodes.remove(node)
            self.keys = {x: y for x, y in sorted(self.keys.items())}
        else:
            raise ValueError
