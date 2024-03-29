![](https://github.com/VitalyRomanov/nhkv/actions/workflows/run-tests.yaml/badge.svg)

# NHKV

NHKV (no hassle key-value) is a library for on-disk key-value storage. The use case is primarily directed towards storing large objects. The goal was to make it lightweight and without external dependencies. Was created primarily for storing datasets for machine learning.

## Installation

```bash
pip install nhkv
```

## Quick Start

```python
from nhkv import KVStore
storage = KVStore("path/to/storage/location")  # a folder is created

key = 0
value = "python serializable object"
storage[key] = value
storage.save()
```

## Available Storage Classes

### SqliteDbDict
Data is stored in Sqlite3 database. The idea id similar to `SqliteDict`. Functionality of `SqliteDbDict` is inferior to `SqliteDict`, but overhead is also smaller.

```python
from nhkv import SqliteDbDict, AutoDbDict

storage = SqliteDbDict("path/to/storage/location", key_type=str)  # a database file is created
# storage = AutoDbDict("path/to/storage/location", backend="sqlite3", key_type=str)
# can also use int keys
storage["string key"] = "python serializable object"
storage.save()  # database transaction is not completed until saved explicitly
# frequent saving affects performance
```

### RocksDbDict and LevelDbDict
There are options to use `rocksdb` and `leveldb`, but need to install these dependencies manually.
```bash
apt install rocksdb leveldb
pip install leveldb python-rocksdb
```
Can import LevelDbDict, RocksDbDict or simply use AutoDbDict (also works with SqliteDbDict).
```python
from nhkv import AutoDbDict, LevelDbDict, RocksDbDict

storage = AutoDbDict("path/to/storage/location", backend="rocksdb")  # a database file is created
storage["string key"] = "python serializable object"
retrieved = storage["string key"]

storage = AutoDbDict("path/to/storage/location", backend="leveldb")  # a database file is created
storage["string key"] = "python serializable object"
retrieved = storage["string key"]
```

### CompactKeyValueStore
The data is kept in mmap file. The index is kept in memory, can become very large if many objects are stored. Mmap files are split in shards.  

```python
from nhkv import CompactKeyValueStore

storage = CompactKeyValueStore(
    "path/to/storage/location",  # folder is created
    serializer=lambda string_: string_.encode("utf8"),  # optional serializer
    deserializer=lambda bytes_: bytes_.decode("utf8"),  # optional deserializer
    shard_size=1048576  # optional shard size in bytes
)  
# key is any type that can be used with `dict`
storage["string key"] = "python serializable object"
storage.save()  # save to ensure transaction is complete
# frequent saving affects performance
storage.close()

storage = CompactKeyValueStore.load("path/to/storage/location")
```

### KVStore
The data is kept in mmap file. The index is kept either in sqlite or shelve database.  

```python
from nhkv import KVStore

storage = KVStore(
    "path/to/storage/location",  # folder is created
    index_backend='sqlite',  # possible options: sqlite | shelve 
    serializer=lambda string_: string_.encode("utf8"),  # optional serializer
    deserializer=lambda bytes_: bytes_.decode("utf8"),  # optional deserializer
    shard_size=1048576  # optional shard size in bytes
)  
# sqlite uses int keys
# shelve uses str keys
storage[100] = "python serializable object"
storage.save()  # save to ensure transaction is complete
# frequent saving affects performance
storage.close()

storage = KVStore.load("path/to/storage/location")
```

## Alternatives

NHKV is closely related to libraries such as 
1. `Shelve` - has non-zero probability of key collision, very slow
2. `SqliteDict` - slower reads and writes, but more functionality (eg. multiprocessing)
3. `DiskCache` - slower reads and writes, but more functionality (eg. cache features)

![Write Time vs Dataset Size](https://i.imgur.com/daem9SK.png)

![Write Time vs Entry Size](https://i.imgur.com/frXNAPA.png)

![Read Time vs Dataset Size](https://i.imgur.com/Tq2naN1.png)

![Read Time vs Entry Size](https://i.imgur.com/R7geN9n.png)

## Limitation

Storage classes in this library are better suited for the batch writes and consecutive batch reads. This represents the intended use case: storing datasets for machine learning. Alternating many reads and writes will result in reduced performance.
