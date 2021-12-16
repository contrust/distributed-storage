from setuptools import setup, find_packages

setup(
    name='storage-server',
    version='0.1.0',
    packages=find_packages(include=['server', 'server.*',
                                    'client', 'client.*',
                                    'database', 'database.*'])
)
