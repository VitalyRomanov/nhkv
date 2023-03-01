import sys
import time

from nhkv import CompactKeyValueStore, DbDict, KVStore
import shelve
import sqlite3


try:
    from sqlitedict import SqliteDict
    from diskcache import Index
    import pandas as pd
except ImportError:
    print("Install extra packages: pip install pandas sqlitedict diskcache")
    sys.exit()


def test_sqlite3_write(texts, datasize, str_len):
    conn = sqlite3.connect(f"sqlite_{datasize}_{str_len}.s3db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS [mydict] ("
                "[key] VARCHAR(255) PRIMARY KEY NOT NULL, "
                "[value] VARCHAR(255) NOT NULL)")
    for i, line in enumerate(texts):
        cur.execute("INSERT INTO [mydict] (key, value) VALUES (?, ?)",
                    (str(i), line))
    conn.commit()
    cur.close()
    conn.close()


def test_shelve_write(texts, datasize, str_len):
    d = shelve.open(f"shelve_{datasize}_{str_len}.shelf", protocol=4)
    for i, line in enumerate(texts):
        d[str(i)] = line
    d.close()


def test_sqlitedict_write(texts, datasize, str_len):
    d = SqliteDict(f'sqlitedict_{datasize}_{str_len}.sqlite')
    for i, line in enumerate(texts):
        d[str(i)] = line
    d.commit()
    d.close()


def test_DbDict_write(texts, datasize, str_len):
    d = DbDict(f"dbdict_{datasize}_{str_len}.s3db")
    for i, line in enumerate(texts):
        d[str(i)] = line
    d.save()
    d.close()


def test_kvstore_write(texts, datasize, str_len):
    d = CompactKeyValueStore(f"compactkv_{datasize}_{str_len}.kv")
    for i, line in enumerate(texts):
        d[i] = line
    d._flush_shards()
    d.close()
    d.save()


def test_diskkvstore_write(texts, datasize, str_len):
    d = KVStore(f"kvstore_{datasize}_{str_len}.diskkv")
    for i, line in enumerate(texts):
        d[i] = line
    d._flush_shards()
    d.close()
    d.save()


def test_diskcache_write(texts, datasize, str_len):
    d = Index(f"diskcache_{datasize}_{str_len}")
    for i, line in enumerate(texts):
        d[i] = line


def test_sqlite3_read(texts, datasize, str_len):
    conn = sqlite3.connect(f"sqlite_{datasize}_{str_len}.s3db")
    cur = conn.cursor()
    for i, text in enumerate(texts):
        cur.execute("select [value] from [mydict] where [key]=?", (str(i),))
        a = cur.fetchone()[0]
        assert a == text
    cur.close()
    conn.close()


def test_shelve_read(texts, datasize, str_len):
    d = shelve.open(f"shelve_{datasize}_{str_len}.shelf", protocol=4)
    for i, text in enumerate(texts):
        a = d[str(i)]
        assert a == text
    d.close()


def test_sqlitedict_read(texts, datasize, str_len):
    d = SqliteDict(f'sqlitedict_{datasize}_{str_len}.sqlite')
    for i, text in enumerate(texts):
        a = d[str(i)]
        assert a == text
    d.close()


def test_DbDict_read(texts, datasize, str_len):
    d = DbDict(f"dbdict_{datasize}_{str_len}.s3db")
    for i, text in enumerate(texts):
        a = d[str(i)]
        assert a == text
    d.close()


def test_kvstore_read(texts, datasize, str_len):
    d = CompactKeyValueStore.load(f"compactkv_{datasize}_{str_len}.kv")
    for i, text in enumerate(texts):
        a = d[i]
        assert a == text
    d.close()


def test_diskkvstore_read(texts, datasize, str_len):
    d = KVStore.load(f"kvstore_{datasize}_{str_len}.diskkv")
    for i, text in enumerate(texts):
        a = d[i]
        assert a == text
    d.close()


def test_diskcache_read(texts, datasize, str_len):
    d = Index(f"diskcache_{datasize}_{str_len}")
    for i, text in enumerate(texts):
        a = d[i]
        assert a == text


class TextReader:
    def __init__(self, path, limit_length=None, datasize=None):
        self.file_path = path
        self.limit_length = limit_length
        self.data_size = datasize
        self.count_lines()

    def count_lines(self):
        self.line_count = 0
        self.average_len = 0
        for line in iter(self):
            self.line_count += 1
            self.average_len += len(line)
        self.average_len /= self.line_count

    def __len__(self):
        return self.line_count

    def __iter__(self):
        file_ = open(self.file_path, "r")
        for ind, line in enumerate(file_):
            if ind > self.data_size:
                break
            if self.limit_length is not None:
                yield line[:self.limit_length]
            else:
                yield

text_path = sys.argv[1]

writes = {
    "data size": [],
    "avg len": [],
    "sqlite3": [],
    # "shelve": [],
    "sqlitedict": [],
    "DbDict": [],
    "CompactKVStore": [],
    "KVStore": [],
    "DiskCache": []
}

prev_len = None
prev_count = None

for str_len in [10, 100, 1000, 3000, 6000, 10000]:
    for datasize in [1e4, 3e4, 6e4, 1e5, 1e6]:

        texts = TextReader(text_path, str_len, datasize)

        if prev_len == texts.average_len and prev_count == texts.line_count:
            break

        prev_len = texts.average_len
        prev_count = texts.line_count

        writes["data size"].append(texts.line_count)
        writes["avg len"].append(texts.average_len)

        start_time = time.time()
        test_sqlite3_write(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        writes["sqlite3"].append(end_time - start_time)
        # writes.append({"Type": "sqlite3", "datasize": datasize, "time": end_time - start_time})

        # start_time = time.time()
        # test_shelve_write(texts, texts.line_count, texts.average_len)
        # end_time = time.time()
        # print("--- %s seconds ---" % (end_time - start_time))
        # writes["shelve"].append(end_time - start_time)
        # # writes.append({"Type": "shelve", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_sqlitedict_write(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        writes["sqlitedict"].append(end_time - start_time)
        # writes.append({"Type": "sqlitedict", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_DbDict_write(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        writes["DbDict"].append(end_time - start_time)
        # writes.append({"Type": "DbDict", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_kvstore_write(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        writes["CompactKVStore"].append(end_time - start_time)
        # writes.append({"Type": "KVStore", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_diskkvstore_write(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        writes["KVStore"].append(end_time - start_time)
        # writes.append({"Type": "KVStore", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_diskcache_write(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        writes["DiskCache"].append(end_time - start_time)
        # writes.append({"Type": "KVStore", "datasize": datasize, "time": end_time - start_time})

writes = pd.DataFrame.from_dict(writes)
# writes = writes.set_index("index")
writes.to_csv("writes.csv")

reads = {
    "data size": [],
    "avg len": [],
    "sqlite3": [],
    # "shelve": [],
    "sqlitedict": [],
    "DbDict": [],
    "CompactKVStore": [],
    "KVStore": [],
    "DiskCache": []
}

prev_len = None
prev_count = None

for str_len in [10, 100, 1000, 3000, 6000, 10000]:
    for datasize in [1e4, 3e4, 6e4, 1e5, 1e6]:
        texts = TextReader(text_path, str_len, datasize)

        if prev_len == texts.average_len and prev_count == texts.line_count:
            break

        prev_len = texts.average_len
        prev_count = texts.line_count

        reads["data size"].append(texts.line_count)
        reads["avg len"].append(texts.average_len)

        start_time = time.time()
        test_sqlite3_read(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        reads["sqlite3"].append(end_time - start_time)
        # reads.append({"Type": "sqlite3", "datasize": datasize, "time": end_time - start_time})

        # start_time = time.time()
        # test_shelve_read(texts, texts.line_count, texts.average_len)
        # end_time = time.time()
        # print("--- %s seconds ---" % (end_time - start_time))
        # reads["shelve"].append(end_time - start_time)
        # # reads.append({"Type": "shelve", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_sqlitedict_read(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        reads["sqlitedict"].append(end_time - start_time)
        # reads.append({"Type": "sqlitedict", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_DbDict_read(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        reads["DbDict"].append(end_time - start_time)
        # reads.append({"Type": "DbDict", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_kvstore_read(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        reads["CompactKVStore"].append(end_time - start_time)
        # reads.append({"Type": "KVStore", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_diskkvstore_read(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        reads["KVStore"].append(end_time - start_time)
        # reads.append({"Type": "KVStore", "datasize": datasize, "time": end_time - start_time})

        start_time = time.time()
        test_diskcache_read(texts, texts.line_count, texts.average_len)
        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))
        reads["DiskCache"].append(end_time - start_time)
        # reads.append({"Type": "KVStore", "datasize": datasize, "time": end_time - start_time})

reads = pd.DataFrame.from_dict(reads)
# reads = reads.set_index("index")
reads.to_csv("reads.csv")

import matplotlib.pyplot as plt

writes = pd.read_csv("writes.csv")
writes_data_size = writes.query("`data size` == 100001")
plt.plot(writes_data_size["avg len"], writes_data_size[["sqlite3", "sqlitedict", "DbDict", "CompactKVStore", "KVStore", "DiskCache"]])
plt.legend(["Sqlite3", "SqliteDict", "DbDict", "CompactStorage", "KVStore", "DiskCache"])
plt.xlabel("Average string length")
plt.ylabel("Time required, s")
plt.gca().set_xscale("log")
plt.title("Write time vs entry size")
plt.savefig("write_time_vs_entry_size.png")
plt.close()

writes_avg_len = writes.query("`avg len` > 100 and `avg len` < 2000")
plt.plot(writes_avg_len["data size"], writes_avg_len[["sqlite3", "sqlitedict", "DbDict", "CompactKVStore", "KVStore", "DiskCache"]])
plt.legend(["Sqlite3", "SqliteDict", "DbDict", "CompactStorage", "KVStore", "DiskCache"])
plt.xlabel("Number of entries")
plt.ylabel("Time required, s")
# plt.gca().set_xscale("log")
plt.title("Write time vs dataset size")
plt.savefig("write_time_vs_dataset_size.png")
plt.close()

reads = pd.read_csv("reads.csv")
reads_data_size = reads.query("`data size` == 100001")
plt.plot(reads_data_size["avg len"], reads_data_size[["sqlite3", "sqlitedict", "DbDict", "CompactKVStore", "KVStore", "DiskCache"]])
plt.legend(["Sqlite3", "SqliteDict", "DbDict", "CompactKVStore", "KVStore", "DiskCache"])
plt.xlabel("Average string length")
plt.ylabel("Time required, s")
plt.gca().set_xscale("log")
plt.title("Read time vs entry size")
plt.savefig("read_time_vs_entry_size.png")
plt.close()

reads_avg_len = reads.query("`avg len` > 100 and `avg len` < 2000")
plt.plot(reads_avg_len["data size"], reads_avg_len[["sqlite3", "sqlitedict", "DbDict", "CompactKVStore", "KVStore", "DiskCache"]])
plt.legend(["Sqlite3", "SqliteDict", "DbDict", "CompactKVStore", "KVStore", "DiskCache"])
plt.xlabel("Number of entries")
plt.ylabel("Time required, s")
plt.title("Read time vs dataset size")
# plt.gca().set_xscale("log")
plt.savefig("read_time_vs_dataset_size.png")
plt.close()