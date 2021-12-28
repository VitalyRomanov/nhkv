# Closet

Library for *almost* native python key-value storage. The use case is primarily directed towards storing large objects.

## Requirements

Key-value store relies on Python's Shelve (or sqlite3) and mmap modules. 

## Alternatives

no_hassle_kv is closely related to libraries such as 
1. Shelve - has non-zero probability of key collision
2. Sqlitedict - slower reads
3. Chest - does not scale as much
4. Shove - requires configuration of the backend storage

Benchmark based on writing 300Mb worth of strings into a key-value storage.

| |Pure Sqlite|Shelve|Sqlitedict|no_hassle_kv.KVStore|
|---|---|---|---|---|
|Batch Import, s|71.49|-|56.35|98.82|
|Batch Readout, s|20.24|-|22.62|5.50|
|Disk Storage Size, Mb|322.3|-|322.7|310.2|


## Installation

```bash
pip install git+https://github.com/VitalyRomanov/no_hassle_kv.git
```

## Usage

```python
from no_hassle_kv import KVStore

storage_path = "~/storage"

...

kv_store = KVStore(storage_path, shard_size=2**30, index_backend="sqlite")
kv_store["string_key"] = large_object

same_object = kv_store["string_key"]
```

## Limitation

Closet uses off-memory index backed by `shelve` or `sqlite`. `shelve` is based on Python's Shelve library. It relies on key hashing and collisions are possible. Additionally, `shelve` storage occupies more space on disk. There are no collisions with `sqlite`, but key value must be string. For large datasets avoiding key collisions might be important. In this case `sqlite` should be used for index backend. In this case, Closet is better suited for the batch writes and consecutive batch reads. Alternating many reads and writes will result in more frequent `commit` calls for sqlite backend and will degrade the performance.
