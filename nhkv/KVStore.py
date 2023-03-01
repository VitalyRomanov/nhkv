import logging
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Union

from nhkv import DbDict
import shelve
import dill as pickle

from nhkv.DbOffsetStorage import DbOffsetStorage
from nhkv.CompactStorage import CompactStorage
import mmap


class CompactKeyValueStore:
    """
    CompactKeyValueStore is a class that can be used as a key-value storage. Offset index is kept in memory. Values
    are stored in mmap file. Values are sharded into separate files.
    """
    _file_index = None
    _index: CompactStorage = None
    _key_map = None
    _is_open = False

    _opened_shards = None
    _shard_for_write = 0
    _written_in_current_shard = 0
    _shard_size = 0

    def __init__(self, path, shard_size=2**30, serializer=None, deserializer=None, **kwargs):
        """
        Initialize CompactKeyValueStore instance.
        :param path: Path to the location, where the storage will be created.
        :param shard_size: Size of a single shard in bytes.
        :param serializer: Function for serializing values. Must return bytes.
        :param deserializer: Function for deserializing values. Takes in a bytes.
        :param kwargs: additional parameters to be passed to offset storage initializer and file index
        initializer
        """
        self.path = Path(path)

        self._init_serializers(serializer, deserializer)
        self._initialize_file_index(shard_size, **kwargs)
        self._initialize_offset_index(**kwargs)

        self._is_open = True

    def _init_serializers(self, serializer, deserializer):
        """
        :param serializer:
        :param deserializer:
        :return:
        """
        if serializer is not None and deserializer is not None:
            self._serialize = serializer
            self._deserialize = deserializer
            return

        if (
                serializer is not None and deserializer is None or
                serializer is None and deserializer is not None
        ):
            logging.warning("Both, serializer and deserializer should be specified. Fallback to pickle.")

        self._serialize = lambda value: pickle.dumps(value, protocol=4, fix_imports=False)
        self._deserialize = lambda value: pickle.loads(value)

    def _initialize_file_index(self, shard_size, **kwargs):
        """
        :param shard_size: shard size in bytes
        :param kwargs: no additional parameters are used at the moment
        :return:
        """
        self._file_index = dict()  # (shard, filename)
        self._opened_shards = OrderedDict()  # (shard, file, mmap object) if mmap is none -> opened for write
        self._shard_for_write = 0
        self._written_in_current_shard = 0
        self._shard_size = shard_size

    def _initialize_offset_index(self, **kwargs):
        """
        Initialize offset storage.
        :param kwargs: no additional parameters are used at the moment.
        :return:
        """
        self._key_map = dict()
        self._index = CompactStorage(3, dtype="L")  # third of space is wasted to shards

    def _init_storage(self, size):
        self._index._active_storage_size = size

    def _increment_byte_count(self, written):
        self._written_in_current_shard += written
        if self._written_in_current_shard >= self._shard_size:
            self._shard_for_write += 1
            self._written_in_current_shard = 0

    def _get_with_id(self, key):
        triplet = self._index[key]
        if triplet is None:
            raise KeyError(f"Key not found: {key}")
        shard, pos, len_ = triplet
        if len_ == 0:
            raise ValueError("Entry length is 0")
        _, mm = self._reading_mode(shard)
        return self._deserialize(mm[pos: pos + len_])

    @staticmethod
    def _get_name_format(id_):
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
        self.path.mkdir(exist_ok=True, parents=True)

    def _close_some_files_if_too_many_opened(self, id_, max_opened_shards_limit=10):
        self._opened_shards.move_to_end(id_, last=True)
        if len(self._opened_shards) >= max_opened_shards_limit:
            _, shard = self._opened_shards.popitem(last=False)
            if shard[1] is not None:
                shard[1].close()
            shard[0].close()

    def _writing_mode(self, id_):
        if id_ not in self._opened_shards:
            if id_ not in self._file_index:
                self._file_index[id_] = self._get_name_format(id_)
            self._opened_shards[id_] = self._open_for_write(self._file_index[id_])
        elif self._opened_shards[id_][1] is not None:  # mmap is None
            self._opened_shards[id_][1].close()
            self._opened_shards[id_][0].close()
            self._opened_shards[id_] = self._open_for_write(self._file_index[id_])

        self._close_some_files_if_too_many_opened(id_)
        return self._opened_shards[id_]

    def _reading_mode(self, id_):
        if id_ not in self._opened_shards:
            if id_ not in self._file_index:
                self._file_index[id_] = self._get_name_format(id_)
            self._opened_shards[id_] = self._open_for_read(self._file_index[id_])
        elif self._opened_shards[id_][1] is None:
            self._opened_shards[id_][0].close()
            self._opened_shards[id_] = self._open_for_read(self._file_index[id_])

        self._close_some_files_if_too_many_opened(id_)
        return self._opened_shards[id_]

    @property
    def _runtime_version(self):
        return f"python_{sys.version_info.major}.{sys.version_info.minor}"

    @property
    def _class_name(self):
        return self.__class__.__name__

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

    def _save_param(self):
        pickle.dump(
            [getattr(self, v) for v in self._get_variables_for_saving()],
            open(self.path.joinpath("store_params"), "wb"), protocol=4
        )

    def _load_param(self):
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
        self._index.save(self.path.joinpath("store_index"))

    def _load_index(self):
        self._index = CompactStorage.load(self.path.joinpath("store_index"))

    def _close_all_shards(self):
        for shard in self._opened_shards.values():
            for s in shard[::-1]:
                if s:
                    s.flush()

    def _flush_shards(self):
        for shard in self._opened_shards.values():
            if shard[1] is not None:
                shard[1].flush()

    def __contains__(self, item):
        raise NotImplementedError("This operation is too expensive. Use `get` instead.")
        # triplet = self._index[item]
        # if triplet is None:
        #     return False
        # return True

    def __setitem__(self, key, value):
        """
        :param key: Keys should be integers
        :param value:
        :return:
        """
        if self._key_map is not None:
            if key in self._key_map:
                key_: Optional[int] = self._key_map[key]
            else:
                key_ = None
        else:
            if not isinstance(key, int):
                raise ValueError("Keys should be integers when no key map is available")
            key_: Optional[int] = key

        serialized = self._serialize(value)

        if key_ is not None:
            try:
                # check if there is an entry with such key
                existing_shard, existing_pos, existing_len = self._index[key_]
            except IndexError:
                pass
            else:
                if len(serialized) == existing_len:
                    # successfully retrieved existing position and can overwrite old data
                    _, mm = self._reading_mode(existing_shard)
                    mm[existing_pos: existing_pos + existing_len] = serialized
                    return

        # the key is new or the data size is different
        f, _ = self._writing_mode(self._shard_for_write)
        position = f.tell()
        written = f.write(serialized)
        to_index = (self._shard_for_write, position, written)
        if key_ is None or key_ == len(self._index):
            index_key = self._index.append(to_index)
            if self._key_map is not None:
                self._key_map[key] = index_key
        else:
            self._index[key_] = to_index

        self._increment_byte_count(written)

    def __getitem__(self, key):
        """
        Get value from key.
        :param key:
        :return:
        """
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

    def __del__(self):
        pass  # throws exception on shutdown
        # self.close()

    def keys(self):
        """
        Get list of keys
        :return:
        """
        if self._key_map is not None:
            return list(self._key_map.keys())
        else:
            return list(range(len(self)))

    def items(self):
        """
        Returns generator for key-value pairs
        :return:
        """
        for key in self.keys():
            yield key, self[key]

    def get(self, key, default):
        """
        Get value by key and return default if key does not exist.
        :param key:
        :param default:
        :return:
        """
        try:
            return self[key]
        except KeyError:
            return default

    def save(self):
        """
        Save all required information for loading later from disk.
        :return:
        """
        self._flush_shards()
        self._save_index()
        self._save_param()

    @classmethod
    def load(cls, path):
        store = cls(path)
        store._load_param()
        store._load_index()
        return store

    def close(self):
        if self._is_open:
            self.save()
            self._close_all_shards()
            self._is_open = False


class KVStore(CompactKeyValueStore):

    _index: Union[DbOffsetStorage, shelve.Shelf] = None

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
            raise NotImplementedError()
            # self._key_type = str
            # self._key_type_error_message = "Key type should be `str` when `sqlite` is used " \
            #                                "for index backend, but {key_type} given."
        elif type(self._index) is DbOffsetStorage:
            self._key_type = int
            self._key_type_error_message = "Key type should be `int` when `sqlite` is used " \
                                           "for index backend, but `{key_type}` given."
        elif type(self._index) is shelve.DbfilenameShelf:
            self._key_type = str
            self._key_type_error_message = "Key type should be `str` when `shelve` is used " \
                                           "for index backend, but `{key_type}` given."
        else:
            raise ValueError("Unknown index type")

    def _verify_key_type(self, key):
        if type(key) != self._key_type:
            raise TypeError(self._key_type_error_message.format(key_type=type(key).__name__))

    def _initialize_offset_index(self, index_backend="sqlite", **kwargs):
        if index_backend is None:
            index_backend = self._infer_backend()
        self._index_backend = index_backend
        self._create_index()

    def _get_shelve_index_path(self, with_siffix=False):
        db_name = "shelve_index"
        initial_name = self.path.joinpath(db_name)
        if not with_siffix:
            return initial_name

        dir_ = initial_name.parent
        if not dir_.is_dir():
            return initial_name
        for item in dir_.iterdir():
            if item.name.startswith(db_name):
                return item
        else:
            return initial_name

    def _get_sqlite_index_path(self):
        return self.path.joinpath("sqlite_index.db")

    def _infer_backend(self):
        if self._get_shelve_index_path(with_siffix=True).is_file():
            return "shelve"
        elif self._get_sqlite_index_path().is_file():
            return "sqlite"
        else:
            raise FileNotFoundError("No index file found.")

    def _get_index_path(self):
        if self._index_backend == "shelve":
            index_path = self._get_shelve_index_path()
        elif self._index_backend == "sqlite":
            index_path = self._get_sqlite_index_path()
        else:
            raise ValueError(
                f"`index_backend` should be `shelve` or `sqlite`, but `{self._index_backend}` is provided."
            )
        return index_path

    def _create_index(self):
        index_path = self._get_index_path()
        index_path.parent.mkdir(exist_ok=True, parents=True)

        if self._index_backend == "shelve":
            self._index = shelve.open(str(index_path.absolute()), protocol=4)
        elif self._index_backend == "sqlite":
            self._index = DbOffsetStorage(index_path)
        else:
            raise ValueError("Unknown index backend")
            # self._index = DbDict(index_path)

    def _save_index(self):
        """
        Flush data to disk
        :return:
        """
        if self._index_backend == "shelve":
            self._index.sync()
        elif self._index_backend == "sqlite":
            self._index.save()
        else:
            raise Exception("Something went wrong")

    def _load_index(self):
        pass

    def __setitem__(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        self._verify_key_type(key)

        serialized = self._serialize(value)

        # no old data or the key is new
        f, _ = self._writing_mode(self._shard_for_write)
        position = f.tell()
        written = f.write(serialized)

        self._index[key] = (self._shard_for_write, position, written)
        self._increment_byte_count(written)

    def __getitem__(self, key):
        """

        :param key:
        :return:
        """
        self._verify_key_type(key)

        # if type(self._index) is DbDict:
        #     if type(key) is not str:
        #         raise TypeError(
        #             f"Key type should be `str` when `sqlite` is used for index backend, but {type(key)} given."
        #         )
        return self._get_with_id(key)

    def __contains__(self, item):
        raise NotImplementedError("This operation is too expensive. Use `get` instead.")

    def keys(self):
        """
        Get list of keys
        :return:
        """
        return list(self._index.keys())

    @classmethod
    def load(cls, path):
        """
        Load previously created storage
        :param path:
        :return:
        """
        store = cls(path, index_backend=None)
        store._load_param()
        return store
