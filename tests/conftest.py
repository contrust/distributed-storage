import os
import pathlib
import shutil

import pytest


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
