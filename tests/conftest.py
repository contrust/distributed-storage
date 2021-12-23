import multiprocessing
import os
import pathlib
import shutil
import time
from contextlib import contextmanager

import pytest

from database.four_parts_hash_storage import FourPartsHashStorage
from hashring.hashring import HashRing
from server.__main__ import run_server
from server.database_request_handler import DatabaseRequestHandler
from server.router_request_handler import RouterRequestHandler

ROUTER_PORT = 2308
DATABASES_PORTS = [2309, 2310, 2311]
DATABASE_NAMES = ['panama', 'liana']


@contextmanager
def _chdir_to_temp_directory():
    temp_directory = pathlib.Path(os.path.abspath(__file__)).parent / pathlib.Path(
        'temp')
    temp_directory.mkdir(exist_ok=True)
    os.chdir(temp_directory)
    yield
    os.chdir(temp_directory.parent)
    shutil.rmtree('temp')


@pytest.fixture(autouse=True, scope="function")
def chdir_to_temp_directory_with_function_scope():
    with _chdir_to_temp_directory() as result:
        yield result


@pytest.fixture(scope="session")
def chdir_to_temp_directory_with_session_scope():
    with _chdir_to_temp_directory() as result:
        yield result


def run_database_server(hostname: str, port: int):
    host = f'{hostname}:{port}'
    databases_names = ['panama', 'liana']
    databases = {}
    for database_name in databases_names:
        databases[database_name] = FourPartsHashStorage(
            pathlib.Path('temp') / pathlib.Path(host) / pathlib.Path(database_name))
    request_handler = DatabaseRequestHandler(databases)
    run_server(request_handler, hostname, port)


def run_router_server(hostname: str, port: int, replicas_number: int,
                      databases_ports: list):
    databases_hosts = [f'localhost:{port}' for port in databases_ports]
    ring = HashRing(databases_hosts, 100, replicas_number)
    request_handler = RouterRequestHandler(ring)
    run_server(request_handler, hostname, port)


@pytest.fixture(autouse=True, scope="session")
def run_servers():
    processes = []
    for database_port in DATABASES_PORTS:
        process = multiprocessing.Process(target=run_database_server,
                                          args=('localhost', database_port))
        processes.append(process)

    process = multiprocessing.Process(target=run_router_server,
                                      args=('localhost', ROUTER_PORT, 1,
                                            DATABASES_PORTS))
    processes.append(process)
    for process in processes:
        process.start()
    time.sleep(2)
    yield
    for process in processes:
        process.terminate()
