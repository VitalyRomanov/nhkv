# NHKV (no hassle KV)

Library for on-disk key-value storage written in Python. The use case is primarily directed towards storing large objects.

## Requirements

Key-value store relies on Python's Shelve (or sqlite3) and mmap modules. 

## Alternatives

NHKV is closely related to libraries such as 
1. Shelve - has non-zero probability of key collision
2. Sqlitedict - slower reads
3. Shove - requires configuration of the backend storage

DbDict, CompactStorage and KVStore are implemented in this library. DbDict is a wrapper for Sqlite3 database. CompactStorage and KVStore use mmap file for storing data. CompactStore uses numpy arrays to store mmap offsets in memory. KVStore stores index in sqlite or shelve database. 

![](https://www.dropbox.com/s/zp9pccodzfq6yy4/write_time_vs_data_size.png?dl=1)
![](https://www.dropbox.com/s/c3clm9h3kibzkaj/write_time_vs_string_length.png?dl=1)
![](https://www.dropbox.com/s/gy7c8mb684j3zqt/read_time_vs_data_size.png?dl=1)
![](https://www.dropbox.com/s/1tss4xl6djyffjg/read_time_vs_string_length.png?dl=1)

## Installation

```bash
pip install git+https://github.com/VitalyRomanov/nhkv.git
```

## Usage

```python
from nhkv import KVStore

storage_path = "~/storage"

...

kv_store = KVStore(storage_path, shard_size=2**30, index_backend="sqlite")
kv_store["string_key"] = large_object

same_object = kv_store["string_key"]
```

## Limitation

Class KVStore is better suited for the batch writes and consecutive batch reads. Alternating many reads and writes will result in more frequent `commit` calls for sqlite backend and will degrade the performance.
