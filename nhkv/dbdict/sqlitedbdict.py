import sqlite3
import pickle
from typing import Union, Type, Optional


class SqliteDbDict:
    """
    SqliteDbDict is a class for storing key-value pairs in Sqlite3 database. Keys can have types `int` or `str`, and
    must be passed to the object constructor. The values are stored as pickled objects.
    """
    STR_KEY_LIMIT = 512
    _is_open = False

    def __init__(self, path, key_type: Union[Type[int], Type[str]] = str, str_key_lim: Optional[int] = None):
        """
        Create a Sqlite3-backed key-value storage
        :param path: path to the location where database file will be created. If the file exists, existing storage is
        loaded
        :param key_type: Possible key types are `int` and `str` (pass Python type names, not strings)
        :param str_key_lim: Maximum length for string keys
        """
        self.path = path
        self._conn = sqlite3.connect(path)
        self._cur = self._conn.cursor()
        self._is_open = True
        self.requires_commit = False
        self._key_type = key_type

        if str_key_lim is not None:
            self.STR_KEY_LIMIT = str_key_lim

        if key_type == str:
            keyt_ = f"VARCHAR({self.STR_KEY_LIMIT})"
        elif key_type == int:
            keyt_ = "INTEGER"
        else:
            raise ValueError("Supported key types are `int`, `str`")

        self._cur.execute(
            "CREATE TABLE IF NOT EXISTS [mydict] ("
            "[key] %s PRIMARY KEY NOT NULL, "
            "[value] BLOB)" % keyt_
        )

    def _check_key_type(self, key):
        if type(key) != self._key_type:
            raise TypeError(f"Declared and provided key types to not match: {type(key)} != {self._key_type}")

    def _str_key_trunc(self, key):
        if len(key) > self.STR_KEY_LIMIT:
            key = key[:self.STR_KEY_LIMIT]
        return key

    def __setitem__(self, key, value):
        self._check_key_type(key)

        if self._key_type is str:
            key = self._str_key_trunc(key)

        val = sqlite3.Binary(pickle.dumps(value, protocol=4))
        self._cur.execute("REPLACE INTO [mydict] (key, value) VALUES (?, ?)",
                          (key, val))

        self.requires_commit = True

    def __getitem__(self, key):
        self._check_key_type(key)

        if self.requires_commit:
            self.save()
            self.requires_commit = False

        if self._key_type is str:
            key = self._str_key_trunc(key)

        self._cur.execute("SELECT value FROM [mydict] WHERE key = ?", (key,))
        resp = self._cur.fetchmany(1)
        if len(resp) == 0:
            raise KeyError("Key not found")
        val = resp[0][0]

        return pickle.loads(bytes(val))

    def __delitem__(self, key):
        try:
            self._conn.execute("DELETE FROM [mydict] WHERE key = ?", (key,))
        except:
            pass

    def __len__(self):
        return self._cur.execute("SELECT COUNT() FROM [mydict]").fetchone()[0]

    def __del__(self):
        self.close()

    def get(self, item, default):
        try:
            return self[item]
        except KeyError:
            return default

    def keys(self):
        keys = self._cur.execute("SELECT key FROM [mydict]").fetchall()
        return list(key[0] for key in keys)

    def save(self):
        self._conn.commit()

    def close(self):
        if self._is_open is True:
            self.save()
            self._cur.close()
            self._conn.close()
            self._is_open = False
