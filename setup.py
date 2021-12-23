from setuptools import setup, find_packages

setup(
    name='distributed_storage',
    version='1.0.0',
    packages=find_packages(include=['server', 'server.*',
                                    'client', 'client.*',
                                    'database', 'database.*',
                                    'hashring', 'hashring*']),
    entry_points={
        'console_scripts': [
            'storage_server = server.__main__:entry_point',
            'storage_client = client.__main__:entry_point',
            'storage_ring = hashring.__main__:entry_point'
        ]
    }
)
