from nhkv.dbdict.abstractdbdict import AbstractDbDict


class RocksDbDict(AbstractDbDict):
    def __init__(self, path, **kwargs):
        super().__init__(path, **kwargs)

    def _initialize_connection(self, path, **kwargs):
        try:
            # noinspection PyPackageRequirements
            import rocksdb
        except ImportError:
            raise ImportError("Install rocksdb: pip install python-rocksdb")
        self._conn = rocksdb.DB(path, rocksdb.Options(create_if_missing=True))

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
        self._conn.put(key, self._serialize(value))

    def __getitem__(self, key):
        key = self._encode_key(key)
        value = self._conn.get(key)
        if value is None:
            raise KeyError(f"Key not found: {self._decode_key(key)}")
        return self._deserialize(value)

    def __delitem__(self, key):
        key = self._encode_key(key)
        self._conn.delete(key)

    def __len__(self):
        return len(self.keys())

    def __del__(self):
        pass

    def keys(self):
        it = self._conn.iterkeys()
        it.seek_to_first()
        return [self._decode_key(key) for key in it]

    def save(self):
        pass

    def close(self):
        pass
