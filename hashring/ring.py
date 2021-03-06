import hashlib
import random


def hash_to_32bit_int(data: bytes):
    return int.from_bytes(hashlib.md5(data).digest()[: 4], 'little')


class HashRing:
    def __init__(self, nodes: iter = None,
                 keys_for_node_count: int = 100, replicas_number: int = 1):
        self.keys_for_node_count = keys_for_node_count
        self.replicas_number = replicas_number
        self.nodes_replicas = {}
        self.keys = {}
        self.nodes = []
        for node in nodes:
            self.add_node(node)

    def get_nodes_ranges(self):
        nodes_ranges = {}
        previous_key = max(self.keys)
        previous_node = self.keys[previous_key]
        for key, node in self.keys.items():
            if previous_node not in nodes_ranges:
                nodes_ranges[previous_node] = []
            nodes_ranges[previous_node].append(tuple([previous_key, key]))
            previous_key = key
            previous_node = node
        return nodes_ranges

    def find_node_for_string(self, data: str):
        data = data.encode('utf-8')
        key_hash = hash_to_32bit_int(data)
        if self.keys:
            max_element = max(self.keys)
            last_element = None
            for ring_key in self.keys:
                if key_hash < ring_key:
                    break
                else:
                    last_element = ring_key
            if last_element:
                node = self.keys[last_element]
            else:
                node = self.keys[max_element]
            return node
        else:
            return None

    def add_node(self, node: str):
        if node not in self.nodes:
            for _ in range(self.keys_for_node_count):
                number = random.randint(0, 2 ** 32)
                if number not in self.keys:
                    self.keys[number] = node
            self.nodes.append(node)
            self._update_replicas()

    def remove_node(self, node: str):
        if node in self.nodes:
            for ring_key in list(self.keys.keys()):
                if self.keys[ring_key] == node:
                    del self.keys[ring_key]
            self.nodes.remove(node)
            self._update_replicas()

    def _update_replicas(self):
        self.keys = {x: y for x, y in sorted(self.keys.items())}
        for i in range(len(self.nodes)):
            self.nodes_replicas[self.nodes[i]] = []
            for j in range(1, self.replicas_number + 1):
                next_node = self.nodes[(i + j) % len(self.nodes)]
                if next_node in self.nodes_replicas[self.nodes[i]] or \
                        next_node == self.nodes[i]:
                    break
                self.nodes_replicas[self.nodes[i]].append(next_node)
