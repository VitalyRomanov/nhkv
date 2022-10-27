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
    file_index = None
    index = None
    key_map = None

    opened_shards = None
    shard_for_write = 0
    written_in_current_shard = 0
    shard_size = 0

    def __init__(self, path, shard_size=2**30, serializer=None, deserializer=None, **kwargs):
        self.path = Path(path)

        self.init_serializers(serializer, deserializer)
        self.initialize_file_index(shard_size, **kwargs)
        self.initialize_offset_index(**kwargs)

    def init_serializers(self, serializer, deserializer):
        if serializer is not None and deserializer is not None:
            self.serialize = serializer
            self.deserialize = deserializer
        else:
            self.serialize = lambda value: pickle.dumps(value, protocol=4)
            self.deserialize = lambda value: pickle.loads(value)

    def initialize_file_index(self, shard_size, **kwargs):
        self.file_index = dict()  # (shard, filename)
        self.opened_shards = dict()  # (shard, file, mmap object) if mmap is none -> opened for write
        self.shard_for_write = 0
        self.written_in_current_shard = 0
        self.shard_size = shard_size

    def initialize_offset_index(self, **kwargs):
        self.key_map = dict()
        self.index = CompactStorage(3, dtype="L")  # third of space is wasted to shards

    def init_storage(self, size):
        self.index.active_storage_size = size

    def __setitem__(self, key, value, key_error='ignore'):
        if self.key_map is not None:
            if key not in self.key_map:
                self.key_map[key] = len(self.index)
            key_: int = self.key_map[key]
        else:
            if not isinstance(key, int):
                raise ValueError("Keys should be integers when setting compact_ensured=True")
            key_: int = key

        # serialized = pickle.dumps(value, protocol=4)
        serialized = self.serialize(value)

        try:
            existing_shard, existing_pos, existing_len = self.index[key_]  # check if there is an entry with such key
        except IndexError:
            pass
        else:
            if len(serialized) == existing_len:
                # successfully retrieved existing position and can overwrite old data
                _, mm = self.reading_mode(existing_shard)
                mm[existing_pos: existing_pos + existing_len] = serialized
                return

        # no old data or the key is new
        f, _ = self.writing_mode(self.shard_for_write)
        position = f.tell()
        written = f.write(serialized)
        self.index.append((self.shard_for_write, position, written))
        self.increment_byte_count(written)

    # def add_posting(self, term_id, postings):
    #     if self.index is None:
    #         raise Exception("Index is not initialized")
    #
    #     serialized = pickle.dumps(postings, protocol=4)
    #
    #     f, _ = self.writing_mode(self.shard_for_write)
    #
    #     position = f.tell()
    #     written = f.write(serialized)
    #     self.index[term_id] = (self.shard_for_write, position, written)
    #     self.increment_byte_count(written)
    #     return term_id

    def increment_byte_count(self, written):
        self.written_in_current_shard += written
        if self.written_in_current_shard >= self.shard_size:
            self.shard_for_write += 1
            self.written_in_current_shard = 0

    def __getitem__(self, key):
        if self.key_map is not None:
            if key not in self.key_map:
                raise KeyError("Key does not exist:", key)
            key_ = self.key_map[key]
        else:
            key_ = key
            if key_ >= len(self.index):
                raise KeyError("Key does not exist:", key)
        try:
            return self.get_with_id(key_)
        except ValueError:
            raise KeyError("Key does not exist:", key)

    def get(self, key, default):
        try:
            return self[key]
        except KeyError:
            return default

    def get_with_id(self, doc_id):
        triplet = self.index[doc_id]
        if triplet is None:
            raise KeyError(f"Key not found: {doc_id}")
        shard, pos, len_ = triplet
        if len_ == 0:
            raise ValueError("Entry length is 0")
        _, mm = self.reading_mode(shard)
        return self.deserialize(mm[pos: pos+len_])
        # return pickle.loads(mm[pos: pos+len_])

    def get_name_format(self, id_):
        return 'store_shard_{0:04d}'.format(id_)

    def open_for_read(self, name):
        # raise file not exists
        f = open(self.path.joinpath(name), "r+b")
        mm = mmap.mmap(f.fileno(), 0)
        return f, mm

    def open_for_write(self, name):
        # raise file not exists
        self.check_dir_exists()
        f = open(self.path.joinpath(name), "ab")
        return f, None

    def check_dir_exists(self):
        if not self.path.is_dir():
            self.path.mkdir()

    def writing_mode(self, id_):
        if id_ not in self.opened_shards:
            if id_ not in self.file_index:
                self.file_index[id_] = self.get_name_format(id_)
            self.opened_shards[id_] = self.open_for_write(self.file_index[id_])
        elif self.opened_shards[id_][1] is not None:  # mmap is None
            self.opened_shards[id_][1].close()
            self.opened_shards[id_][0].close()
            self.opened_shards[id_] = self.open_for_write(self.file_index[id_])
        return self.opened_shards[id_]

    def reading_mode(self, id_):
        if id_ not in self.opened_shards:
            if id_ not in self.file_index:
                self.file_index[id_] = self.get_name_format(id_)
            self.opened_shards[id_] = self.open_for_read(self.file_index[id_])
        elif self.opened_shards[id_][1] is None:
            self.opened_shards[id_][0].close()
            self.opened_shards[id_] = self.open_for_read(self.file_index[id_])
        return self.opened_shards[id_]

    @property
    def runtime_version(self):
        return f"python_{sys.version_info.major}.{sys.version_info.minor}"

    def get_class_name(self):
        return self.__class__.__name__

    @property
    def class_name(self):
        return self.get_class_name()

    @staticmethod
    def get_variables_for_saving():
        return [
            "runtime_version",
            "class_name",
            "file_index",
            "shard_for_write",
            "written_in_current_shard",
            "shard_size",
            "path",
            "key_map"
        ]

    def save_param(self):
        pickle.dump(
            [getattr(self, v) for v in self.get_variables_for_saving()],
            open(self.path.joinpath("store_params"), "wb"), protocol=4
        )

    def load_param(self):
        params = pickle.load(open(self.path.joinpath("store_params"), "rb"))
        variable_names = self.get_variables_for_saving()

        assert len(params) == len(variable_names)
        runtime_version = params.pop(0)
        class_name = params.pop(0)
        assert runtime_version == self.runtime_version
        assert class_name == self.class_name

        for name, var in zip(variable_names[2:], params):
            setattr(self, name, var)

    def save_index(self):
        pickle.dump(self.index, open(self.path.joinpath("store_index"), "wb"), protocol=4)

    def load_index(self):
        self.index = pickle.load(open(self.path.joinpath("store_index"), "rb"))

    def save(self):
        self.save_index()
        self.save_param()
        self.close_all_shards()

    @classmethod
    def load(cls, path):
        store = cls(path)
        store.load_param()
        store.load_index()
        return store

    def close_all_shards(self):
        for shard in self.opened_shards.values():
            for s in shard[::-1]:
                if s:
                    s.close()

    def close(self):
        self.close_all_shards()

    def commit(self):
        for shard in self.opened_shards.values():
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
        self.check_dir_exists()
        self._infer_key_type()

    def _infer_key_type(self):
        if type(self.index) is DbDict:
            self._key_type = str
            self._key_type_error_message = "Key type should be `str` when `sqlite` is used for index backend, but {key_type} given."
        elif type(self.index) is DbOffsetStorage:
            self._key_type = int
            self._key_type_error_message = "Key type should be `int` when `sqlite` is used for index backend, but {key_type} given."
        elif type(self.index) is shelve.DbfilenameShelf:
            self._key_type = str
            self._key_type_error_message = "Key type should be `str` when `shelve` is used for index backend, but {key_type} given."
        else:
            raise ValueError("Unknown index type")

    def _verify_key_type(self, key_type):
        if key_type != self._key_type:
            raise ValueError(self._key_type_error_message.format(key_type=key_type))

    def initialize_offset_index(self, index_backend="sqlite", **kwargs):
        if index_backend is None:
            index_backend = self.infer_backend()
        self.create_index(self.get_index_path(index_backend))

    def infer_backend(self):
        if self.path.joinpath("store_index.shelve.db").is_file():
            return "shelve"
        elif self.path.joinpath("store_index.s3db").is_file():
            return "sqlite"
        else:
            raise FileNotFoundError("No index file found.")

    def get_index_path(self, index_backend):
        if index_backend == "shelve":
            index_path = self.path.joinpath("store_index.shelve")
        elif index_backend == "sqlite":
            index_path = self.path.joinpath("store_index.s3db")
        else:
            raise ValueError(f"`index_backend` should be `shelve` or `sqlite`, but `{index_backend}` is provided.")
        return index_path

    def create_index(self, index_path):

        parent = index_path.parent
        if not parent.is_dir():
            parent.mkdir()

        if index_path.name.endswith(".shelve"):
            self.index = shelve.open(str(index_path.absolute()), protocol=4)
        else:
            self.index = DbOffsetStorage(index_path)
            # self.index = DbDict(index_path)

    def __setitem__(self, key, value, key_error='ignore'):
        self._verify_key_type(type(key))

        # serialized = pickle.dumps(value, protocol=4)
        serialized = self.serialize(value)

        # try:
        #     existing_shard, existing_pos, existing_len = self.index[key] # check if there is an entry with such key
        # except KeyError:
        #     pass
        # else:
        #     if len(serialized) == existing_len:
        #         # successfully retrieved existing position and can overwrite old data
        #         _, mm = self.reading_mode(existing_shard)
        #         mm[existing_pos: existing_pos + existing_len] = serialized
        #         return

        # no old data or the key is new
        f, _ = self.writing_mode(self.shard_for_write)
        position = f.tell()
        written = f.write(serialized)

        self.index[key] = (self.shard_for_write, position, written)
        self.increment_byte_count(written)

    def __getitem__(self, key):
        if type(self.index) is DbDict:
            if type(key) is not str:
                raise TypeError(
                    f"Key type should be `str` when `sqlite` is used for index backend, but {type(key)} given."
                )
        return self.get_with_id(key)

    def save_index(self):
        self.commit()

    def commit(self):
        super(KVStore, self).commit()
        if type(self.index) in {DbDict, DbOffsetStorage}:
            self.index.commit()  # for DbDict index
        else:
            self.index.sync()

    @classmethod
    def load(cls, path):
        store = cls(path, index_backend=None)
        store.load_param()
        return store

    def load_index(self):
        pass
