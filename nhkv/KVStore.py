import sys
from pathlib import Path
from typing import Optional

from nhkv import DbDict
import shelve
import dill as pickle

from nhkv.DbOffsetStorage import DbOffsetStorage
from nhkv.CompactStorage import CompactStorage
import mmap


class CompactKeyValueStore:
    _file_index = None
    _index = None
    _key_map = None

    _opened_shards = None
    _shard_for_write = 0
    _written_in_current_shard = 0
    _shard_size = 0

    def __init__(self, path, shard_size=2**30, serializer=None, deserializer=None, **kwargs):
        self.path = Path(path)

        self._init_serializers(serializer, deserializer)
        self._initialize_file_index(shard_size, **kwargs)
        self._initialize_offset_index(**kwargs)

    def _init_serializers(self, serializer, deserializer):
        if serializer is not None and deserializer is not None:
            self._serialize = serializer
            self._deserialize = deserializer
        else:
            self._serialize = lambda value: pickle.dumps(value, protocol=4)
            self._deserialize = lambda value: pickle.loads(value)

    def _initialize_file_index(self, shard_size, **kwargs):
        self._file_index = dict()  # (shard, filename)
        self._opened_shards = dict()  # (shard, file, mmap object) if mmap is none -> opened for write
        self._shard_for_write = 0
        self._written_in_current_shard = 0
        self._shard_size = shard_size

    def _initialize_offset_index(self, **kwargs):
        self._key_map = dict()
        self._index = CompactStorage(3, dtype="L")  # third of space is wasted to shards

    def _init_storage(self, size):
        self._index._active_storage_size = size

    def __setitem__(self, key, value, key_error='ignore'):
        if self._key_map is not None:
            if key not in self._key_map:
                self._key_map[key] = len(self._index)
            key_: int = self._key_map[key]
        else:
            if not isinstance(key, int):
                raise ValueError("Keys should be integers when setting compact_ensured=True")
            key_: int = key

        # serialized = pickle.dumps(value, protocol=4)
        serialized = self._serialize(value)

        try:
            existing_shard, existing_pos, existing_len = self._index[key_]  # check if there is an entry with such key
        except IndexError:
            pass
        else:
            if len(serialized) == existing_len:
                # successfully retrieved existing position and can overwrite old data
                _, mm = self._reading_mode(existing_shard)
                mm[existing_pos: existing_pos + existing_len] = serialized
                return

        # no old data or the key is new
        f, _ = self._writing_mode(self._shard_for_write)
        position = f.tell()
        written = f.write(serialized)
        self._index.append((self._shard_for_write, position, written))
        self._increment_byte_count(written)

    def _increment_byte_count(self, written):
        self._written_in_current_shard += written
        if self._written_in_current_shard >= self._shard_size:
            self._shard_for_write += 1
            self._written_in_current_shard = 0

    def __getitem__(self, key):
        if self._key_map is not None:
            if key not in self._key_map:
                raise KeyError("Key does not exist:", key)
            key_ = self._key_map[key]
        else:
            key_ = key
            if key_ >= len(self._index):
                raise KeyError("Key does not exist:", key)
        try:
            return self._get_with_id(key_)
        except ValueError:
            raise KeyError("Key does not exist:", key)

    def __len__(self):
        return len(self._index)

    def get(self, key, default):
        try:
            return self[key]
        except KeyError:
            return default

    def _get_with_id(self, doc_id):
        triplet = self._index[doc_id]
        if triplet is None:
            raise KeyError(f"Key not found: {doc_id}")
        shard, pos, len_ = triplet
        if len_ == 0:
            raise ValueError("Entry length is 0")
        _, mm = self._reading_mode(shard)
        return self._deserialize(mm[pos: pos + len_])
        # return pickle.loads(mm[pos: pos+len_])

    def _get_name_format(self, id_):
        return 'store_shard_{0:04d}'.format(id_)

    def _open_for_read(self, name):
        # raise file not exists
        f = open(self.path.joinpath(name), "r+b")
        mm = mmap.mmap(f.fileno(), 0)
        return f, mm

    def _open_for_write(self, name):
        # raise file not exists
        self._check_dir_exists()
        f = open(self.path.joinpath(name), "ab")
        return f, None

    def _check_dir_exists(self):
        if not self.path.is_dir():
            self.path.mkdir()

    def _writing_mode(self, id_):
        if id_ not in self._opened_shards:
            if id_ not in self._file_index:
                self._file_index[id_] = self._get_name_format(id_)
            self._opened_shards[id_] = self._open_for_write(self._file_index[id_])
        elif self._opened_shards[id_][1] is not None:  # mmap is None
            self._opened_shards[id_][1].close()
            self._opened_shards[id_][0].close()
            self._opened_shards[id_] = self._open_for_write(self._file_index[id_])
        return self._opened_shards[id_]

    def _reading_mode(self, id_):
        if id_ not in self._opened_shards:
            if id_ not in self._file_index:
                self._file_index[id_] = self._get_name_format(id_)
            self._opened_shards[id_] = self._open_for_read(self._file_index[id_])
        elif self._opened_shards[id_][1] is None:
            self._opened_shards[id_][0].close()
            self._opened_shards[id_] = self._open_for_read(self._file_index[id_])
        return self._opened_shards[id_]

    @property
    def _runtime_version(self):
        return f"python_{sys.version_info.major}.{sys.version_info.minor}"

    def _get_class_name(self):
        return self.__class__.__name__

    @property
    def _class_name(self):
        return self._get_class_name()

    @staticmethod
    def _get_variables_for_saving():
        return [
            "_runtime_version",
            "_class_name",
            "_file_index",
            "_shard_for_write",
            "_written_in_current_shard",
            "_shard_size",
            "path",
            "_key_map"
        ]

    def save_param(self):
        pickle.dump(
            [getattr(self, v) for v in self._get_variables_for_saving()],
            open(self.path.joinpath("store_params"), "wb"), protocol=4
        )

    def load_param(self):
        params = pickle.load(open(self.path.joinpath("store_params"), "rb"))
        variable_names = self._get_variables_for_saving()

        assert len(params) == len(variable_names)
        runtime_version = params.pop(0)
        class_name = params.pop(0)
        assert runtime_version == self._runtime_version
        assert class_name == self._class_name

        for name, var in zip(variable_names[2:], params):
            setattr(self, name, var)

    def _save_index(self):
        pickle.dump(self._index, open(self.path.joinpath("store_index"), "wb"), protocol=4)

    def _load_index(self):
        self._index = pickle.load(open(self.path.joinpath("store_index"), "rb"))

    def save(self):
        self._save_index()
        self.save_param()
        self._close_all_shards()

    @classmethod
    def load(cls, path):
        store = cls(path)
        store.load_param()
        store._load_index()
        return store

    def _close_all_shards(self):
        for shard in self._opened_shards.values():
            for s in shard[::-1]:
                if s:
                    s.close()

    def close(self):
        self._close_all_shards()

    def commit(self):
        for shard in self._opened_shards.values():
            if shard[1] is not None:
                shard[1].flush()


class KVStore(CompactKeyValueStore):
    def __init__(
            self, path, shard_size=2 ** 30, serializer=None, deserializer=None,
            index_backend: Optional[str] = "sqlite", **kwargs
    ):
        """
        Create a disk backed key-value storage.
        :param path: Location on the disk.
        :param shard_size: Size of storage partition in bytes.
        :param index_backend: Backend for storing the index. Available backends are `shelve` and `sqlite`. `shelve` is
            based on Python's shelve library. It relies on key hashing and collisions are possible. Additionally,
            `shelve` storage occupies more space on disk. There is no collisions with `sqlite`, but key value is must
            be string.
        """
        super().__init__(
            path, shard_size, serializer=serializer, deserializer=deserializer, index_backend=index_backend, **kwargs
        )
        self._check_dir_exists()
        self._infer_key_type()

    def _infer_key_type(self):
        if type(self._index) is DbDict:
            self._key_type = str
            self._key_type_error_message = "Key type should be `str` when `sqlite` is used for index backend, but {key_type} given."
        elif type(self._index) is DbOffsetStorage:
            self._key_type = int
            self._key_type_error_message = "Key type should be `int` when `sqlite` is used for index backend, but {key_type} given."
        elif type(self._index) is shelve.DbfilenameShelf:
            self._key_type = str
            self._key_type_error_message = "Key type should be `str` when `shelve` is used for index backend, but {key_type} given."
        else:
            raise ValueError("Unknown index type")

    def _verify_key_type(self, key_type):
        if key_type != self._key_type:
            raise ValueError(self._key_type_error_message.format(key_type=key_type))

    def _initialize_offset_index(self, index_backend="sqlite", **kwargs):
        if index_backend is None:
            index_backend = self._infer_backend()
        self._create_index(self._get_index_path(index_backend))

    def _infer_backend(self):
        if self.path.joinpath("store_index.shelve.db").is_file():
            return "shelve"
        elif self.path.joinpath("store_index.s3db").is_file():
            return "sqlite"
        else:
            raise FileNotFoundError("No index file found.")

    def _get_index_path(self, index_backend):
        if index_backend == "shelve":
            index_path = self.path.joinpath("store_index.shelve")
        elif index_backend == "sqlite":
            index_path = self.path.joinpath("store_index.s3db")
        else:
            raise ValueError(f"`index_backend` should be `shelve` or `sqlite`, but `{index_backend}` is provided.")
        return index_path

    def _create_index(self, index_path):

        parent = index_path.parent
        if not parent.is_dir():
            parent.mkdir()

        if index_path.name.endswith(".shelve"):
            self._index = shelve.open(str(index_path.absolute()), protocol=4)
        else:
            self._index = DbOffsetStorage(index_path)
            # self._index = DbDict(index_path)

    def __setitem__(self, key, value, key_error='ignore'):
        self._verify_key_type(type(key))

        serialized = self._serialize(value)

        # no old data or the key is new
        f, _ = self._writing_mode(self._shard_for_write)
        position = f.tell()
        written = f.write(serialized)

        self._index[key] = (self._shard_for_write, position, written)
        self._increment_byte_count(written)

    def __getitem__(self, key):
        if type(self._index) is DbDict:
            if type(key) is not str:
                raise TypeError(
                    f"Key type should be `str` when `sqlite` is used for index backend, but {type(key)} given."
                )
        return self._get_with_id(key)

    def _save_index(self):
        self.commit()

    def commit(self):
        super(KVStore, self).commit()
        if type(self._index) in {DbDict, DbOffsetStorage}:
            self._index.commit()  # for DbDict index
        else:
            self._index.sync()

    @classmethod
    def load(cls, path):
        store = cls(path, index_backend=None)
        store.load_param()
        return store

    def _load_index(self):
        pass
