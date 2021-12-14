import pytest
import os
import pathlib
import shutil

import storage_server
from kvstorage.one_key_one_file_storage import OneKeyOneFileStorage
from storage_server.database_request_handler import DatabaseRequestHandler
from storage_server.router_request_handler import RouterRequestHandler
from storage_server.server import run_server


@pytest.fixture(scope="function")
def chdir_to_temp_directory():
    temp_directory = pathlib.Path('./temp')
    tests_directory = os.path.dirname(os.path.abspath(__file__))
    os.chdir(tests_directory)
    temp_directory.mkdir()
    os.chdir(temp_directory)
    yield
    os.chdir(tests_directory)
    shutil.rmtree('temp')


@pytest.fixture(autouse=True, scope="session")
def run_three_database_and_one_router_servers_with_three_databases(chdir_to_temp_directory):
    databases_names = ['panama1', 'dinosaur', 'pringles']
    databases = {}
    for database_name in databases_names:
        database_path = pathlib.Path(database_name)
        database_path.mkdir()
        databases[database_name] = OneKeyOneFileStorage(database_path)
    run_server(DatabaseRequestHandler(databases), 'localhost', 2021)
    run_server(DatabaseRequestHandler(databases), 'localhost', 2022)
    run_server(DatabaseRequestHandler(databases), 'localhost', 2023)
    run_server(RouterRequestHandler(
        ['localhost:2021', 'localhost:2022', 'localhost:2023']),
        'localhost', 2020)
