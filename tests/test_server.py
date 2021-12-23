import pathlib

from server.__main__ import main
from server.database_config import DatabaseConfig
from server.router_config import RouterConfig


def test_db_conf_file_is_created_after_get_config():
    db_conf_name = 'db_conf'
    db_conf_path = pathlib.Path(db_conf_name)
    assert not db_conf_path.exists()
    args = f'-db -g {db_conf_name}'.split()
    main(args)
    assert db_conf_path.exists()


def test_db_conf_file_is_created_with_standard_parameters():
    db_conf_name = 'db_conf'
    db_conf = DatabaseConfig()
    args = f'-db -g {db_conf_name}'.split()
    main(args)
    db_conf.load(db_conf_name)
    assert db_conf.port == 2020
    assert db_conf.hostname == 'localhost'
    assert db_conf.databases == {'dbname': 'db/path'}


def test_rt_conf_file_is_created_after_get_config():
    db_conf_name = 'rt_conf'
    db_conf_path = pathlib.Path(db_conf_name)
    assert not db_conf_path.exists()
    args = f'-rt -g {db_conf_name}'.split()
    main(args)
    assert db_conf_path.exists()


def test_rt_conf_file_is_created_with_standard_parameters():
    rt_conf_name = 'rt_conf'
    rt_conf = RouterConfig()
    args = f'-rt -g {rt_conf_name}'.split()
    main(args)
    rt_conf.load(rt_conf_name)
    assert rt_conf.port == 2020
    assert rt_conf.hostname == 'localhost'
    assert rt_conf.hash_ring_path == 'hashring.pkl'
