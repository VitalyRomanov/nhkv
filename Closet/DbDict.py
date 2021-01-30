import sqlite3
import pickle


class DbDict:
    def __init__(self, path, keytype=str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()
        self.requires_commit = False

        if keytype == str:
            keyt_ = "VARCHAR(255)"
        elif keytype == int:
            keyt_ = "INTEGER"
        else:
            raise ValueError("Keytype only supports str and int")

        self.cur.execute("CREATE TABLE IF NOT EXISTS [mydict] ("
                         "[key] %s PRIMARY KEY NOT NULL, "
                         "[value] BLOB)" % keyt_)

    def __setitem__(self, key, value):
        val = sqlite3.Binary(pickle.dumps(value, protocol=4))
        self.cur.execute("REPLACE INTO [mydict] (key, value) VALUES (?, ?)",
                         (key, val))

        self.requires_commit = True


    def __getitem__(self, key):
        if self.requires_commit:
            self.commit()
            self.requires_commit = False

        self.cur.execute("SELECT value FROM [mydict] WHERE key = ?", (key,))
        resp = self.cur.fetchmany(1)
        if len(resp) == 0:
            raise KeyError("Key not found")
        val = resp[0][0]

        # try:
        #     val = next(iter(self.conn.execute("SELECT value FROM [mydict] WHERE key = ?", (key,))))[0]
        # except StopIteration:
        #     raise KeyError("Key not found")

        return pickle.loads(bytes(val))

    def get(self, item, default):
        try:
            return self[item]
        except KeyError:
            return default

    def __delitem__(self, key):
        try:
            self.conn.execute("DELETE FROM [mydict] WHERE key = ?", (key,))
        except:
            pass

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()