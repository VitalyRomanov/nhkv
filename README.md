# Closet

Library for *almost* native python key-value storage. The use case is primarily directed towards storing large objects.

## Requirements

Key-value store relies on Python's shelve (or sqlite3) and mmap modules. 

## Alternatives

Closet is closely related to libraries such as 
1. Shelve - has non-zero probability of key collision
2. Sqlitedict - slower
3. Chest - does not scale as much
4. Shove - requires configuration of the backend storage

Benchmark based on writing 100Mb worth of strings into a key-value storage.

| |Pure Sqlite|Shelve|Sqlitedict|Closet|
|---|---|---|---|---|
|Batch Import, s|0.7690|0.95|1.91|0.28|
|Batch Readout, s|1.20|0.47|1.91|0.036 (0.14 with `sqlite` backend)|
|Disk Storage Size, Mb|107.4|~34Gb|107.5|103.8|

Disclamer: Cleset's read performance is so much greater primarily to hashing in `shelve` library.



## Installation

```bash
pip install git+https://github.com/VitalyRomanov/Closet.git
```

## Usage

```python
from Closet import KVStore

storage_path = "~/storage"

...

kv_store = KVStore(storage_path, shard_size=2**30, index_backend='sqlite')
kv_store["string_key"] = large_object

same_object = kv_store["string_key"]
```

## Limitation

Coset ises off-memory index backed by `shelve` or `sqlite`. `shelve` is based on Python's shelve library. It relies on key hashing and collisions are possible. Additionally, `shelve` storage occupies more space on disk. There is no collisions with `sqlite`, but key value must be string.

If for large datasets avoiding key collisions might be important. In this case `sqlite` should be used for index backend. In this case, Closet is better suited for batch writes and consecutive batch reads. Alterating many reads and writes will result in more frequent `commit` calls for sqlite backend and will degrade the performance.
