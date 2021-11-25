from setuptools import setup, find_packages

setup(
    name='storage-storage-server',
    version='0.1.0',
    packages=find_packages(include=['storage-server', 'storage-server.*',
                                    'storage-client', 'storage-client.*'])
)
