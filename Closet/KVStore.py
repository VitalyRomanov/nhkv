from Closet import DbDict
import shelve
import os
import pickle

from Closet.DbOffsetStorage import DbOffsetStorage
from Closet.CompactStorage import CompactStorage
import mmap


class CompactKeyValueStore:
    file_index = None
    index = None
    key_map = None

    def __init__(self, path, init_size=100000, shard_size=2**30, compact_ensured=False):

        self.path = path

        self.file_index = dict()  # (shard, filename)

        if compact_ensured:
            self.index = CompactStorage(3, init_size, volatile_access=True)
        else:
            self.key_map = dict()
            self.index = CompactStorage(3, init_size)

        self.opened_shards = dict()  # (shard, file, mmap object) if mmap is none -> opened for write

        self.shard_for_write = 0
        self.written_in_current_shard = 0
        self.shard_size = shard_size

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

        serialized_doc = pickle.dumps(value, protocol=4)

        try:
            existing_shard, existing_pos, existing_len = self.index[key_]  # check if there is an entry with such key
        except IndexError:
            pass
        else:
            if len(serialized_doc) == existing_len:
                # successfully retrieved existing position and can overwrite old data
                _, mm = self.reading_mode(existing_shard)
                mm[existing_pos: existing_pos + existing_len] = serialized_doc
                return

        # no old data or the key is new
        f, _ = self.writing_mode(self.shard_for_write)
        position = f.tell()
        written = f.write(serialized_doc)
        if self.index.volatile_access:  # no key_map available, index directly by key
            if key_ >= len(self.index):
                # make sure key densely follow each other, otherwise a lot
                # of space is wasted
                self.index.resize_storage(int(key_ * 1.2))
            # access with key_ directly
            self.index[key_] = (self.shard_for_write, position, written)
        else:
            # append, index length is maintained by self.index itself
            self.index.append((self.shard_for_write, position, written))
        self.increment_byte_count(written)

    # def add_posting(self, term_id, postings):
    #     if self.index is None:
    #         raise Exception("Index is not initialized")
    #
    #     serialized_doc = pickle.dumps(postings, protocol=4)
    #
    #     f, _ = self.writing_mode(self.shard_for_write)
    #
    #     position = f.tell()
    #     written = f.write(serialized_doc)
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

    def get_with_id(self, doc_id):
        shard, pos, len_ = self.index[doc_id]
        if len_ == 0:
            raise ValueError("Entry length is 0")
        _, mm = self.reading_mode(shard)
        return pickle.loads(mm[pos: pos+len_])

    def get_name_format(self, id_):
        return 'postings_shard_{0:04d}'.format(id_)

    def open_for_read(self, name):
        # raise file not exists
        f = open(os.path.join(self.path, name), "r+b")
        mm = mmap.mmap(f.fileno(), 0)
        return f, mm

    def open_for_write(self, name):
        # raise file not exists
        self.check_dir_exists()
        f = open(os.path.join(self.path, name), "ab")
        return f, None

    def check_dir_exists(self):
        if not os.path.isdir(self.path):
            os.mkdir(self.path)

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

    def save_param(self):
        pickle.dump((
            self.file_index,
            self.shard_for_write,
            self.written_in_current_shard,
            self.shard_size,
            self.path,
            self.key_map
        ), open(os.path.join(self.path, "postings_params"), "wb"), protocol=4)

    def load_param(self):
        self.file_index,\
            self.shard_for_write,\
            self.written_in_current_shard,\
            self.shard_size,\
            self.path, \
            self.key_map = pickle.load(open(os.path.join(self.path, "postings_params"), "rb"))

    def save_index(self):
        pickle.dump(self.index, open(os.path.join(self.path, "postings_index"), "wb"), protocol=4)

    def load_index(self):
        self.index = pickle.load(open(os.path.join(self.path, "postings_index"), "rb"))

    def save(self):
        self.save_index()
        self.save_param()
        self.close_all_shards()

    @classmethod
    def load(cls, path):
        postings = CompactKeyValueStore(path)
        postings.load_param()
        postings.load_index()
        return postings

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
    def __init__(self, path, shard_size=2 ** 30, index_backend="sqlite"):
        """
        Create a disk backed key-value storage.
        :param path: Location on the disk.
        :param shard_size: Size of storage partition in bytes.
        :param index_backend: Backend for storing the index. Available backends are `shelve` and `sqlite`. `shelve` is
            based on Python's shelve library. It relies on key hashing and collisions are possible. Additionally,
            `shelve` storage occupies more space on disk. There is no collisions with `sqlite`, but key value is must
            be string.
        """

        self.path = path

        self.file_index = dict()  # (shard, filename)

        if not os.path.isdir(path):
            os.mkdir(path)

        if index_backend is None:
            index_backend = self.infer_backend()

        self.create_index(self.get_index_path(index_backend))

        self.opened_shards = dict()  # (shard, file, mmap object) if mmap is none -> opened for write

        self.shard_for_write = 0
        self.written_in_current_shard = 0
        self.shard_size = shard_size

    def infer_backend(self):
        if os.path.isfile(os.path.join(self.path, "index.shelve.db")):
            return "shelve"
        elif os.path.isfile(os.path.join(self.path, "index.s3db")):
            return "sqlite"
        else:
            raise FileNotFoundError("No index file found.")

    def get_index_path(self, index_backend):
        if index_backend == "shelve":
            index_path = os.path.join(self.path, "index.shelve")
        elif index_backend == "sqlite":
            index_path = os.path.join(self.path, "index.s3db")
        else:
            raise ValueError(f"`index_backend` should be `shelve` or `sqlite`, but `{index_backend}` is provided.")
        return index_path

    def create_index(self, index_path):
        if index_path.endswith(".shelve"):
            self.index = shelve.open(index_path, protocol=4)
        else:
            self.index = DbOffsetStorage(index_path)
            # self.index = DbDict(index_path)

    def __setitem__(self, key, value, key_error='ignore'):
        if type(self.index) is DbDict:
            if type(key) is not str:
                raise TypeError(
                    f"Key type should be `str` when `sqlite` is used for index backend, but {type(key)} given."
                )
        serialized_doc = pickle.dumps(value, protocol=4)

        # try:
        #     existing_shard, existing_pos, existing_len = self.index[key] # check if there is an entry with such key
        # except KeyError:
        #     pass
        # else:
        #     if len(serialized_doc) == existing_len:
        #         # successfully retrieved existing position and can overwrite old data
        #         _, mm = self.reading_mode(existing_shard)
        #         mm[existing_pos: existing_pos + existing_len] = serialized_doc
        #         return

        # no old data or the key is new
        f, _ = self.writing_mode(self.shard_for_write)
        position = f.tell()
        written = f.write(serialized_doc)

        self.index[key] = (self.shard_for_write, position, written)
        self.increment_byte_count(written)

    def __getitem__(self, key):
        if type(self.index) is DbDict:
            if type(key) is not str:
                raise TypeError(
                    f"Key type should be `str` when `sqlite` is used for index backend, but {type(key)} given."
                )
        return self.get_with_id(key)

    def save(self):
        self.save_param()
        self.close_all_shards()

    def commit(self):
        super(KVStore, self).commit()
        if type(self.index) in {DbDict, DbOffsetStorage}:
            self.index.commit()  # for DbDict index
        else:
            self.index.sync()

    @classmethod
    def load(cls, path):
        postings = KVStore(path, index_backend=None)
        postings.load_param()
        return postings
