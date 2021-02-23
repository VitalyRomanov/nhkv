from Closet import CompactKeyValueStore, DbDict
import shelve
import os
import pickle

class KVStore(CompactKeyValueStore):
    def __init__(self, path, shard_size=2 ** 30, index_backend="shelve"):
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
            self.index = DbDict(index_path)

    def __setitem__(self, key, value, key_error='ignore'):
        if type(self.index) is DbDict:
            if type(key) is not str:
                raise TypeError(f"Key type should be `str` when `sqlite` is used for index backend, but {type(key)} given.")
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
                raise TypeError(f"Key type should be `str` when `sqlite` is used for index backend, but {type(key)} given.")
        return self.get_with_id(key)

    def save(self):
        self.save_param()
        self.close_all_shards()

    def commit(self):
        super(KVStore, self).commit()
        if type(self.index) is DbDict:
            self.index.commit()  # for DbDict index
        else:
            self.index.sync()

    @classmethod
    def load(cls, path):
        postings = KVStore(path, index_backend=None)
        postings.load_param()
        return postings