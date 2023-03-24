from nhkv.dbdict.leveldbdict import LevelDbDict
from nhkv.dbdict.rocksdbdict import RocksDbDict
from nhkv.dbdict.sqlitedbdict import SqliteDbDict


# noinspection PyPep8Naming
def AutoDbDict(path, backend, **kwargs):
    if backend == "sqlite3":
        return SqliteDbDict(path, **kwargs)
    if backend == "rocksdb":
        return RocksDbDict(path, **kwargs)
    if backend == "leveldb":
        return LevelDbDict(path, **kwargs)
