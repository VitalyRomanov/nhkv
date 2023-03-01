import sqlite3


class DbOffsetStorage:
    """
    DbOffsetStorage class creates sqlite3 storage to keep mmap offsets for KVStore. Key for added entries has
    integer type. Meant for internal use.
    """

    _is_open = False

    def __init__(self, path):
        """
        Creates a DbOffsetStorage instance.
        :param path: Path to the dataset file. If exists, existing database is loaded.
        """
        self.path = path

        self._db = sqlite3.connect(path)
        self._cur = self._db.cursor()
        self._cur.execute(
            "CREATE TABLE IF NOT EXISTS offset_storage ("
            "key INTEGER PRIMARY KEY NOT NULL UNIQUE, "
            "shard INTEGER NOT NULL, "
            "position INTEGER NOT NULL, "
            "bytes INTEGER NOT NULL)"
        )

        self._is_open = True
        self.requires_commit = False
        self.added_without_commit = 0

    def _add_item(self, key, value, how="REPLACE"):
        """
        Add entry to the database.
        :param key: Key is an integer ID.
        :param value: Value is a tuple (shard_id, seek_position, len_bytes).
        :param how: Specifies how new entries are added. The default value `REPLACE` ensured added key IDs
        are unique. Can use `INSERT` to make insertion faster, but need to guarantee key uniqueness in this case,
        otherwise an exception is raised by Sqlite. Automatic commits every 100000 records. Otherwise, need to
        call method `commit` manually.
        :return:
        """
        if type(key) is not int:
            raise TypeError("Key type should be int but given: ", type(key))
        shard, position, bytes = value
        self._cur.execute(
            f"{how} INTO offset_storage (key, shard, position, bytes) VALUES (?,?,?,?)",
            (key, shard, position, bytes)
        )
        self.requires_commit = True
        self.added_without_commit += 1
        if self.added_without_commit > 100000:
            self.save()

    def __setitem__(self, key, value):
        """
        Add new entry to the storage or replace the old one.
        :param key: Key is an integer ID.
        :param value: Value is a tuple (shard_id, seek_position, len_bytes).
        :return:
        """
        self._add_item(key, value, how="REPLACE")

    def __getitem__(self, key):
        """
        Retrieve a record from storage.
        :param key: Key is an integer ID.
        :return:
        """
        if self.requires_commit:
            self.save()
        response = self._cur.execute(
            f"SELECT shard, position, bytes FROM offset_storage WHERE key = ?", (key,)
        ).fetchone()
        if response is None:
            raise KeyError()
        return response

    def __contains__(self, item):
        raise NotImplementedError("Use method `get` instead")

    def __len__(self):
        if self.requires_commit:
            self.save()
        return self._cur.execute("SELECT COUNT() FROM offset_storage").fetchone()[0]

    def __del__(self):
        self.close()

    def append(self, key, value):
        """
        Append new entry to the storage.
        :param key: Key is an integer ID. Need to guarantee that the key does not exist. Otherwise, exception is
        raised by Sqlite.
        :param value: Value is a tuple (shard_id, seek_position, len_bytes).
        :return:
        """
        self._add_item(key, value, how="INSERT")

    def get(self, item, default):
        try:
            return self[item]
        except KeyError:
            return default

    def keys(self):
        keys = self._cur.execute("SELECT key FROM offset_storage").fetchall()
        return list(key[0] for key in keys)

    def save(self):
        self._db.commit()
        self.requires_commit = False
        self.added_without_commit = 0

    def close(self):
        if self._is_open:
            self.save()
            self._cur.connection.close()
            self._is_open = False



def test_db_offset_storage():
    db_path = "db_offset_storage.db"
    storage = DbOffsetStorage(db_path)
    storage[0] = (1, 2, 3)
    storage[1] = (4, 5, 6)
    storage[10] = (4, 5, 6)
    storage.save()

    assert storage[0] == (1, 2, 3)
    assert storage[1] == (4, 5, 6)

    try:
        temp = storage[2]
        assert False, "Exception is not caught"
    except KeyError:
        pass

    assert len(storage) == 3

    storage[1] = (5, 5, 6)

    assert storage.get(2, 3) == 3

    assert len(storage) == 3

    assert storage.keys() == [0, 1, 10]

    try:
        storage["cat"] = 5
        assert False, "Exception is not caught"
    except TypeError:
        pass

    del storage

    import os
    os.remove(db_path)
