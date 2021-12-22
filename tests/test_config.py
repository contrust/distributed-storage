from os import path

from server.config import Config


def create_database_config_with_some_parameters():
    hostname = 'localhost'
    port = 2020
    config = Config(hostname, port)
    return config


def test_create_file_after_unload():
    config = create_database_config_with_some_parameters()
    unload_path = 'config_file'
    config.unload(unload_path)
    assert path.exists(unload_path)


def test_config_does_not_change_after_loading():
    config = create_database_config_with_some_parameters()
    unload_path = 'config_file'
    config.unload(unload_path)
    loaded_config = Config()
    loaded_config.load(unload_path)
    assert config.__dict__ == loaded_config.__dict__
