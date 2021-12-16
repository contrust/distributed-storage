import hashlib
import pprint
import random


class HashRing:
    @staticmethod
    def hash_to_32bit_int(data: bytes):
        return int.from_bytes(hashlib.md5(data).digest()[: 4], 'little')

    @staticmethod
    def get_keys_migrations_for_new_nodes_from_old_nodes(old_hash_ring,
                                                         new_hash_ring):
        keys_ranges = {}
        old_nodes = old_hash_ring.nodes
        new_nodes = new_hash_ring.nodes
        previous_not_old_node = None
        previous_old_node = None
        previous_key = None
        previous_node = None
        for ring_key, node in sorted(new_hash_ring.keys.items(), reverse=True):
            if not previous_key:
                previous_key = ring_key
                previous_node = node
            if node in old_nodes and old_nodes[node] == new_nodes[node]:
                if not previous_old_node:
                    previous_old_node = node
            else:
                if not previous_not_old_node:
                    previous_not_old_node = node
            if previous_not_old_node and previous_old_node:
                break
        if previous_old_node and previous_not_old_node:
            for ring_key, node in new_hash_ring.keys.items():
                if previous_node == previous_not_old_node:
                    if previous_old_node not in keys_ranges:
                        keys_ranges[previous_old_node] = {}
                    if previous_not_old_node not in keys_ranges[previous_old_node]:
                        keys_ranges[previous_old_node][previous_not_old_node] = []
                    keys_ranges[previous_old_node][previous_not_old_node].append(
                        tuple([previous_key, ring_key]))
                previous_key = ring_key
                if node in old_nodes and old_nodes[node] == new_nodes[node]:
                    previous_old_node = node
                    previous_node = node
                else:
                    previous_not_old_node = node
                    previous_node = node
        return keys_ranges

    def __init__(self, nodes: iter,
                 keys_for_node_count: int = 100, replicas_number: int = 1):
        self.keys_for_node_count = keys_for_node_count
        self.replicas_number = replicas_number
        self.nodes_replicas = {}
        self.keys = {}
        self.nodes = {}
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
        key_hash = HashRing.hash_to_32bit_int(data)
        print(data, key_hash)
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
            raise ValueError

    def add_node(self, node: str):
        if node not in self.nodes:
            for _ in range(self.keys_for_node_count):
                number = random.randint(0, 2 ** 32)
                if number not in self.keys:
                    self.keys[number] = node
            self.nodes[node] = random.randint(0, 2 ** 32)
            self.keys = {x: y for x, y in sorted(self.keys.items())}
            nodes_list = list(self.nodes.keys())
            for i in range(len(nodes_list)):
                self.nodes_replicas[nodes_list[i]] = []
                for j in range(1, self.replicas_number + 1):
                    next_node = nodes_list[(i + j) % len(nodes_list)]
                    if next_node in self.nodes_replicas[nodes_list[i]] or next_node == nodes_list[i]:
                        break
                    self.nodes_replicas[nodes_list[i]].append(next_node)

        else:
            raise ValueError

    def remove_node(self, node: str):
        if node in self.nodes:
            for ring_key in list(self.keys.keys()):
                if self.keys[ring_key] == node:
                    del self.keys[ring_key]
            del self.nodes[node]
            self.keys = {x: y for x, y in sorted(self.keys.items())}
            nodes_list = list(self.nodes.keys())
            for i in range(len(nodes_list)):
                self.nodes_replicas[nodes_list[i]] = []
                for j in range(1, self.replicas_number + 1):
                    next_node = nodes_list[(i + j) % len(nodes_list)]
                    if next_node in self.nodes_replicas[nodes_list[i]] or next_node == nodes_list[i]:
                        break
                    self.nodes_replicas[nodes_list[i]].append(next_node)
        else:
            raise ValueError


if __name__ == '__main__':
    h = HashRing(['1', '2', '3', '4'], 2, 2)
    old_keys = {x: y for x, y in h.keys.items()}
    old_nodes = {x: y for x, y in h.nodes.items()}
    h2 = HashRing([], 2, 2)
    h2.keys = old_keys
    h2.nodes = old_nodes
    h2.add_node('5')
    print(h.keys)
    pprint.pprint(h.get_nodes_ranges())
    print(h2.keys)
    pprint.pprint(h2.get_nodes_ranges())
