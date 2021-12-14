from setuptools import setup, find_packages

setup(
    name='storage-storage_server',
    version='0.1.0',
    packages=find_packages(include=['storage_server', 'storage_server.*',
                                    'storage_client', 'storage_client.*',
                                    'kvstorage', 'kvstorage.*'])
)
