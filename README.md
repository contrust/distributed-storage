# distributed-storage

## Install

```sh
pip install . -r requirements.txt
```

## Usage

* Create database servers' config files and specify their hostname, port, database names and paths.
* Create hashring object with some parameters and save it in the file, and then add database servers' hosts in ring.
* Create router server config and specify its hostname, port and path to hashring file.
* Run database and router servers with their configs in any order.
* Get, add, remove keys in databases with client module.

| Command | Description |
| --- | --- |
| storage_server -h | show server module help |
| storage_client -h | show client module help |
| storage_ring -h | show hashring module help |

## Run tests

```sh
pytest
```

## Author

**Artyom Borisov**
