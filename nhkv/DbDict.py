import sqlite3
import pickle


class DbDict:
    STR_KEY_LIMIT = 512

    def __init__(self, path, keytype=str, str_key_lim=None):
        self.path = path
        self._conn = sqlite3.connect(path)
        self._cur = self._conn.cursor()
        self.requires_commit = False
        self._key_type = keytype

        if str_key_lim is not None:
            self.STR_KEY_LIMIT = str_key_lim

        if keytype == str:
            keyt_ = f"VARCHAR({self.STR_KEY_LIMIT})"
        elif keytype == int:
            keyt_ = "INTEGER"
        else:
            raise ValueError("Keytype only supports str and int")

        self._cur.execute("CREATE TABLE IF NOT EXISTS [mydict] ("
                         "[key] %s PRIMARY KEY NOT NULL, "
                         "[value] BLOB)" % keyt_)

    def __setitem__(self, key, value):
        if self._key_type is str:
            key = self._str_key_trunc(key)

        val = sqlite3.Binary(pickle.dumps(value, protocol=4))
        self._cur.execute("REPLACE INTO [mydict] (key, value) VALUES (?, ?)",
                          (key, val))

        self.requires_commit = True

    def _str_key_trunc(self, key):
        if len(key) > self.STR_KEY_LIMIT:
            key = key[:self.STR_KEY_LIMIT]
        return key

    def __getitem__(self, key):
        if self.requires_commit:
            self.commit()
            self.requires_commit = False

        if self._key_type is str:
            key = self._str_key_trunc(key)

        self._cur.execute("SELECT value FROM [mydict] WHERE key = ?", (key,))
        resp = self._cur.fetchmany(1)
        if len(resp) == 0:
            raise KeyError("Key not found")
        val = resp[0][0]

        return pickle.loads(bytes(val))

    def get(self, item, default):
        try:
            return self[item]
        except KeyError:
            return default

    def __delitem__(self, key):
        try:
            self._conn.execute("DELETE FROM [mydict] WHERE key = ?", (key,))
        except:
            pass

    def __len__(self):
        return self._cur.execute("SELECT COUNT() FROM [mydict]").fetchone()[0]

    def keys(self):
        keys = self._cur.execute("SELECT key FROM [mydict]").fetchall()
        return list(key[0] for key in keys)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._cur.close()
        self._conn.close()
