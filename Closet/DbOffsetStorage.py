import sqlite3


class DbOffsetStorage:
    def __init__(self, path):
        self.path = path

        self.db = sqlite3.connect(path)
        self.cur = self.db.cursor()
        self.cur.execute("CREATE TABLE IF NOT EXISTS offset_storage ("
                         "key TEXT PRIMARY KEY NOT NULL UNIQUE, "
                         "shard INTEGER NOT NULL, "
                         "position INTEGER NOT NULL, "
                         "bytes INTEGER NOT NULL)")

        self.requires_commit = False

    def __setitem__(self, key, value):
        if type(key) is not int:
            raise TypeError("Key type should be int but given: ", type(key))
        shard, position, bytes = value
        self.cur.execute(
            "REPLACE INTO offset_storage (key, shard, position, bytes) VALUES (?,?,?,?)",
            (key, shard, position, bytes)
        )
        self.requires_commit = True

    def __getitem__(self, key):
        if self.requires_commit:
            self.commit()
        return self.cur.execute(f"SELECT shard, position, bytes FROM offset_storage WHERE key = ?", (key,)).fetchone()

    def commit(self):
        self.db.commit()
        self.requires_commit = False

    def __len__(self):
        if self.requires_commit:
            self.commit()
        return self.cur.execute("SELECT COUNT() FROM offset_storage").fetchone()[0]


def test_DbOffsetStorage():
    storage = DbOffsetStorage("db_offset_storage.db")
    storage[0] = (1, 2, 3)
    storage[1] = (4, 5, 6)

    assert storage[0] == (1, 2, 3)
    assert storage[1] == (4, 5, 6)
