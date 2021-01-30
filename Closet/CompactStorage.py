import numpy as np
import pickle
import mmap
import os


class CompactStorage:
    def __init__(self, n_fields=1, init_size=100000, dtype=np.uint32, volatile_access=False):
        if n_fields == 0:
            raise ValueError("The parameter n_fields should be greater than 0")

        self.volatile_access = volatile_access

        if n_fields == 1:
            self.storage = np.zeros(shape=(init_size, ), dtype=dtype)
        else:
            self.storage = np.zeros(shape=(init_size, n_fields), dtype=dtype)

        self.active_storage_size = 0

    def resize_storage(self, new_size):
        if len(self.storage.shape) == 1:
            self.storage.resize((new_size,))
        else:
            self.storage.resize((new_size,self.storage.shape[1]))

    def __len__(self):
        if self.volatile_access:
            return self.storage.shape[0]
        else:
            return self.active_storage_size

    def __getitem__(self, item):
        if item >= len(self):
            raise IndexError("Out of range:", item)

        if len(self.storage.shape) == 1:
            return self.storage[item]
        else:
            return tuple(self.storage[item])

    def __setitem__(self, key, value):
        # if self.volatile_access:
        #     if key >= len(self):
        #         self.resize_storage(int(key*1.2))

        if key >= len(self):
            # if self.volatile_access:
            #     raise IndexError("Out of range:", key, "Preallocate when using volatile_access=True")
            raise IndexError("Out of range:", key)

        if len(self.storage.shape) == 1:
            self.storage[key] = value
        else:
            self.storage[key,:] = np.fromiter(value, dtype=self.storage.dtype)

    def append(self, value):
        if self.volatile_access:
            raise Exception("Use __setitem__ when volatile_access=True")

        if self.active_storage_size >= self.storage.shape[0]:
            self.resize_storage(int(self.storage.shape[0]*1.2))

        if len(self.storage.shape) == 1:
            self.storage[self.active_storage_size] = value
        else:
            self.storage[self.active_storage_size,:] = np.fromiter(value, dtype=self.storage.dtype)

        self.active_storage_size += 1

class CompactKeyValueStore:
    file_index = None
    index = None
    key_map = None

    def __init__(self, path, init_size=100000, shard_size=2**30, compact_ensured=False):

        self.path = path

        self.file_index = dict() # (shard, filename)


        if compact_ensured:
            self.index = CompactStorage(3, init_size, volatile_access=True)
        else:
            self.key_map = dict()
            self.index = CompactStorage(3, init_size)

        self.opened_shards = dict() # (shard, file, mmap object) if mmap is none -> opened for write

        self.shard_for_write = 0
        self.written_in_current_shard = 0
        self.shard_size=shard_size

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
            existing_shard, existing_pos, existing_len = self.index[key_] # check if there is an entry with such key
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
        if self.index.volatile_access: # no key_map available, index directly by key
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
            raise ValueError("Entry lenght is 0")
        _, mm = self.reading_mode(shard)
        return pickle.loads(mm[pos: pos+len_])

    def get_name_format(self, id_):
        return 'postings_shard_{0:04d}'.format(id_)

    def open_for_read(self, name):
        # raise filenotexists
        f = open(os.path.join(self.path, name), "r+b")
        mm = mmap.mmap(f.fileno(), 0)
        return f, mm

    def open_for_write(self, name):
        # raise filenotexists
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
        elif self.opened_shards[id_][1] is not None: # mmap is None
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