from setuptools import setup, find_packages

setup(
    name='distributed_storage',
    version='1.0.0',
    packages=find_packages(include=['server', 'server.*',
                                    'client', 'client.*',
                                    'database', 'database.*'])
)
