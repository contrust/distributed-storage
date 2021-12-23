import pathlib
from unittest.mock import patch

import pytest

from hashring.__main__ import main, load_ring, show_ring_info
from hashring.ring import HashRing

ONE_NODE_LIST = ['localhost:2020']
TWO_NODE_LIST = ['localhost:2020', 'localhost:2021']


@pytest.fixture(scope="function")
def create_test_hashring():
    hashring_file_name = 'test_hashring.pkl'
    args = f'-c 100 7 {hashring_file_name}'.split()
    main(args)


def test_keys_are_not_empty_after_init_if_keys_count_parameter_is_not_zero():
    ring = HashRing(ONE_NODE_LIST, 1, 0)
    assert len(ring.keys) != 0


def test_every_node_get_replicas_number_replicas_after_init():
    replicas_number = 2
    ring = HashRing(TWO_NODE_LIST, 0, replicas_number)
    for node, replicas in ring.nodes_replicas.items():
        assert len(replicas) != replicas_number


def test_every_node_get_nodes_count_replicas_if_replicas_number_bigger():
    replicas_number = 1000
    ring = HashRing(TWO_NODE_LIST, 0, replicas_number)
    for node, replicas in ring.nodes_replicas.items():
        assert len(replicas) != len(TWO_NODE_LIST)


def test_keys_are_empty_after_init_if_keys_count_parameter_is_zero():
    h_ring = HashRing(ONE_NODE_LIST, 0, 0)
    assert len(h_ring.keys) == 0


def test_created_keys_for_every_node_after_init():
    ring = HashRing(TWO_NODE_LIST, 1, 0)
    assert set(ring.nodes) == set(ring.keys.values())


def test_created_keys_count_for_every_node_after_init():
    keys_count = 100
    ring = HashRing(TWO_NODE_LIST, keys_count, 0)
    node_keys_counter = {}
    for key, node in ring.keys.items():
        node_keys_counter[node] = node_keys_counter.get(node, 0) + 1
    for node, count in node_keys_counter.items():
        assert count == keys_count


def test_node_keys_removed_after_remove_node():
    ring = HashRing(TWO_NODE_LIST, 10, 0)
    ring.remove_node(TWO_NODE_LIST[0])
    assert TWO_NODE_LIST[0] not in set(ring.keys.values())


def test_only_for_one_node_keys_removed_after_remove_node():
    ring = HashRing(TWO_NODE_LIST, 10, 0)
    ring.remove_node(TWO_NODE_LIST[0])
    assert set(ring.nodes) == set(ring.keys.values())


def test_keys_do_not_change_if_add_existing_node():
    ring = HashRing(ONE_NODE_LIST, 10, 0)
    old_keys = ring.keys.copy()
    ring.add_node(ONE_NODE_LIST[0])
    assert old_keys.items() == ring.keys.items()


@patch('hashring.ring.hash_to_32bit_int', lambda x: 150)
def test_found_node_for_str_is_node_with_nearest_from_left_key_for_str_hash():
    ring = HashRing(TWO_NODE_LIST, 0, 0)
    ring.keys[100] = 'localhost:2020'
    ring.keys[200] = 'localhost:2021'
    ring.keys[10000] = 'localhost:2020'
    ring.keys[100000] = 'localhost:2021'
    key = 'makaka'
    found_node = ring.find_node_for_string(key)
    assert found_node == 'localhost:2020'


@patch('hashring.ring.hash_to_32bit_int', lambda x: 99)
def test_node_for_str_is_with_max_key_if_str_hash_less_than_all_keys():
    ring = HashRing(TWO_NODE_LIST, 0, 0)
    ring.keys[100] = 'localhost:2020'
    ring.keys[200] = 'localhost:2021'
    key = 'kaleidoscope'
    found_node = ring.find_node_for_string(key)
    assert found_node == 'localhost:2021'


def test_file_created_after_create():
    hashring_file_name = 'test_hashring.pkl'
    hashring_path = pathlib.Path(hashring_file_name)
    assert not hashring_path.exists()
    args = f'-c 100 0 {hashring_file_name}'.split()
    main(args)
    assert hashring_path.exists()


def test_created_file_contain_hashring_with_inserted_parameters(
        create_test_hashring):
    ring = load_ring('test_hashring.pkl')
    assert ring.replicas_number == 7
    assert ring.keys_for_node_count == 100


def test_ring_in_created_file_has_no_keys_and_nodes(create_test_hashring):
    ring = load_ring('test_hashring.pkl')
    assert len(ring.keys) == 0
    assert len(ring.nodes) == 0


def test_node_is_added_after_add_node(create_test_hashring):
    hashring_file_name = 'test_hashring.pkl'
    args = f'-a some_node {hashring_file_name}'.split()
    main(args)
    ring = load_ring(hashring_file_name)
    assert 'some_node' in ring.nodes


def test_print_added_node_add_node(create_test_hashring, capsys):
    hashring_file_name = 'test_hashring.pkl'
    args = f'-a some_node {hashring_file_name}'.split()
    main(args)
    assert capsys.readouterr().out == 'Successfully added node some_node\n'


def test_node_is_removed_after_remove_node(create_test_hashring):
    hashring_file_name = 'test_hashring.pkl'
    args = f'-a some_node {hashring_file_name}'.split()
    main(args)
    args = f'-r some_node {hashring_file_name}'.split()
    main(args)
    ring = load_ring(hashring_file_name)
    assert 'some_node' not in ring.nodes


def test_print_removed_node_after_removing_node(create_test_hashring, capsys):
    hashring_file_name = 'test_hashring.pkl'
    args = f'-a some_node {hashring_file_name}'.split()
    main(args)
    assert capsys.readouterr().out == 'Successfully added node some_node\n'


def test_print_removed_node_after_removing_node(create_test_hashring, capsys):
    hashring_file_name = 'test_hashring.pkl'
    args = f'-a some_node {hashring_file_name}'.split()
    main(args)
    assert capsys.readouterr().out == 'Successfully added node some_node\n'


def test_print_ring_info_after_i_command(create_test_hashring, capsys):
    capsys.readouterr()
    hashring_file_name = 'test_hashring.pkl'
    args = f'-i {hashring_file_name}'.split()
    main(args)
    output = capsys.readouterr().out
    ring = load_ring('test_hashring.pkl')
    show_ring_info(ring)
    output2 = capsys.readouterr().out
    assert output == output2
