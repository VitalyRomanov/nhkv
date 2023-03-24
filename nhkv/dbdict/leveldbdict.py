from nhkv.dbdict.abstractdbdict import AbstractDbDict


class LevelDbDict(AbstractDbDict):
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

    def _initialize_connection(self, path, **kwargs):
        try:
            # noinspection PyPackageRequirements
            import leveldb
        except ImportError:
            raise ImportError("Install leveldb: pip install leveldb")
        self._conn = leveldb.LevelDB(path, create_if_missing=True)

    @classmethod
    def _check_key_type(cls, key):
        if type(key) != str:
            raise TypeError(f"Key type should be `str`")

    @classmethod
    def _encode_key(cls, key):
        cls._check_key_type(key)
        return key.encode("utf-8")

    @classmethod
    def _decode_key(cls, key):
        return key.decode("utf-8")

    def __setitem__(self, key, value):
        key = self._encode_key(key)
        self._conn.Put(key, self._serialize(value))

    def __getitem__(self, key):
        key = self._encode_key(key)
        return self._deserialize(self._conn.Get(key))

    def __delitem__(self, key):
        key = self._encode_key(key)
        self._conn.Delete(key)

    def __len__(self):
        it = self._conn.RangeIter()
        count = 0
        for _ in it:
            count += 1
        return count

    def __del__(self):
        self.close()

    def keys(self):
        it = self._conn.RangeIter()
        return [self._decode_key(i[0]) for i in it]

    def save(self):
        pass

    def close(self):
        pass
