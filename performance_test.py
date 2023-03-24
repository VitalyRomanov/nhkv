import sys
import time
from collections import defaultdict

from nhkv import CompactKeyValueStore, SqliteDbDict, KVStore
import shelve
import sqlite3


try:
    from sqlitedict import SqliteDict
    from diskcache import Index
    import leveldb
    import rocksdb
    import pandas as pd
    import matplotlib.pyplot as plt
except ImportError as e:
    print(e)
    print("Install extra packages: pip install pandas sqlitedict diskcache matplotlib leveldb python-rocksdb")
    sys.exit()


def sqlite3_write(texts, datasize, str_len):
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


def sqlite3_read(texts, datasize, str_len):
    conn = sqlite3.connect(f"sqlite_{datasize}_{str_len}.s3db")
    cur = conn.cursor()
    for i, text in enumerate(texts):
        cur.execute("select [value] from [mydict] where [key]=?", (str(i),))
        a = cur.fetchone()[0]
        assert a == text
    cur.close()
    conn.close()


def shelve_write(texts, datasize, str_len):
    d = shelve.open(f"shelve_{datasize}_{str_len}.shelf", protocol=4)
    for i, line in enumerate(texts):
        d[str(i)] = line
    d.close()


def shelve_read(texts, datasize, str_len):
    d = shelve.open(f"shelve_{datasize}_{str_len}.shelf", protocol=4)
    for i, text in enumerate(texts):
        a = d[str(i)]
        assert a == text
    d.close()


def sqlitedict_write(texts, datasize, str_len):
    d = SqliteDict(f'sqlitedict_{datasize}_{str_len}.sqlite', outer_stack=False, autocommit=False)
    for i, line in enumerate(texts):
        d[str(i)] = line
    d.commit()
    d.close()


def sqlitedict_read(texts, datasize, str_len):
    d = SqliteDict(f'sqlitedict_{datasize}_{str_len}.sqlite', outer_stack=False, autocommit=False)
    for i, text in enumerate(texts):
        a = d[str(i)]
        assert a == text
    d.close()


def DbDict_write(texts, datasize, str_len):
    d = SqliteDbDict(f"dbdict_{datasize}_{str_len}.s3db")
    for i, line in enumerate(texts):
        d[str(i)] = line
    d.save()
    d.close()


def DbDict_read(texts, datasize, str_len):
    d = SqliteDbDict(f"dbdict_{datasize}_{str_len}.s3db")
    for i, text in enumerate(texts):
        a = d[str(i)]
        assert a == text
    d.close()

def compactkvstore_write(texts, datasize, str_len):
    d = CompactKeyValueStore(f"compactkv_{datasize}_{str_len}.kv")
    for i, line in enumerate(texts):
        d[i] = line
    d._flush_shards()
    d.close()
    d.save()


def compactkvstore_read(texts, datasize, str_len):
    d = CompactKeyValueStore.load(f"compactkv_{datasize}_{str_len}.kv")
    for i, text in enumerate(texts):
        a = d[i]
        assert a == text
    d.close()


def kvstore_write(texts, datasize, str_len):
    d = KVStore(f"kvstore_{datasize}_{str_len}.diskkv")
    for i, line in enumerate(texts):
        d[i] = line
    d._flush_shards()
    d.close()
    d.save()


def kvstore_read(texts, datasize, str_len):
    d = KVStore.load(f"kvstore_{datasize}_{str_len}.diskkv")
    for i, text in enumerate(texts):
        a = d[i]
        assert a == text
    d.close()


def diskcache_write(texts, datasize, str_len):
    d = Index(f"diskcache_{datasize}_{str_len}")
    for i, line in enumerate(texts):
        d[i] = line


def diskcache_read(texts, datasize, str_len):
    d = Index(f"diskcache_{datasize}_{str_len}")
    for i, text in enumerate(texts):
        a = d[i]
        assert a == text


def leveldb_write(texts, datasize, str_len):
    db = leveldb.LevelDB(f"leveldb_{datasize}_{str_len}", create_if_missing=True)
    for i, line in enumerate(texts):
        db.Put(f"{i}".encode("utf-8"), line.encode("utf-8"), sync=False)
    # db


def leveldb_read(texts, datasize, str_len):
    db = leveldb.LevelDB(f"leveldb_{datasize}_{str_len}")
    for i, text in enumerate(texts):
        a = db.Get(f"{i}".encode("utf-8")).decode("utf-8")
        assert a == text


def rocksdb_write(texts, datasize, str_len):
    db = rocksdb.DB(f"rocksdb_{datasize}_{str_len}", rocksdb.Options(create_if_missing=True))
    for i, line in enumerate(texts):
        db.put(f"{i}".encode("utf-8"), line.encode("utf-8"), sync=False)
    # db


def rocksdb_read(texts, datasize, str_len):
    db = rocksdb.DB(f"rocksdb_{datasize}_{str_len}", rocksdb.Options(create_if_missing=False))
    for i, text in enumerate(texts):
        a = db.get(f"{i}".encode("utf-8")).decode("utf-8")
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


def benchmark_writes(text_path, test_str_lens, test_num_entries, storage_types):

    writes = defaultdict(list)

    prev_len = None
    prev_count = None

    for str_len in test_str_lens:
        for datasize in test_num_entries:

            texts = TextReader(text_path, str_len, datasize)

            if prev_len == texts.average_len and prev_count == texts.line_count:
                break

            prev_len = texts.average_len
            prev_count = texts.line_count

            writes["data size"].append(texts.line_count)
            writes["avg len"].append(texts.average_len)

            for storage_type in storage_types:
                write_fn = write_fns[storage_type]
                start_time = time.time()
                write_fn(texts, texts.line_count, texts.average_len)
                end_time = time.time()
                print("--- %s seconds ---" % (end_time - start_time))
                writes[storage_type].append(end_time - start_time)

    writes = pd.DataFrame.from_dict(writes)
    writes.to_csv("writes.csv", index=False)


def benchmark_reads(text_path, test_str_lens, test_num_entries, storage_types):
    reads = defaultdict(list)

    prev_len = None
    prev_count = None

    for str_len in test_str_lens:
        for datasize in test_num_entries:

            texts = TextReader(text_path, str_len, datasize)
            if prev_len == texts.average_len and prev_count == texts.line_count:
                break

            prev_len = texts.average_len
            prev_count = texts.line_count

            reads["data size"].append(texts.line_count)
            reads["avg len"].append(texts.average_len)

            for storage_type in storage_types:

                read_fn = read_fns[storage_type]

                start_time = time.time()
                read_fn(texts, texts.line_count, texts.average_len)
                end_time = time.time()
                print("--- %s seconds ---" % (end_time - start_time))
                reads[storage_type].append(end_time - start_time)

    reads = pd.DataFrame.from_dict(reads)
    reads.to_csv("reads.csv", index=False)


def plot_writes():
    writes = pd.read_csv("writes.csv")
    available_storage = [k for k in writes.columns if k not in {"data size", "avg len"}]
    writes_data_size = writes.query("`data size` == 100001")
    plt.plot(writes_data_size["avg len"],
             writes_data_size[available_storage])
    plt.legend(available_storage)
    plt.xlabel("Average string length")
    plt.ylabel("Time required, s")
    plt.gca().set_xscale("log")
    plt.title("Write time vs entry size")
    plt.savefig("write_time_vs_entry_size.png")
    plt.close()

    writes_avg_len = writes.query("`avg len` > 100 and `avg len` < 2000")
    plt.plot(writes_avg_len["data size"],
             writes_avg_len[available_storage])
    plt.legend(available_storage)
    plt.xlabel("Number of entries")
    plt.ylabel("Time required, s")
    # plt.gca().set_xscale("log")
    plt.title("Write time vs dataset size")
    plt.savefig("write_time_vs_dataset_size.png")
    plt.close()


def plot_reads():
    reads = pd.read_csv("reads.csv")
    available_storage = [k for k in reads.columns if k not in {"data size", "avg len"}]
    reads_data_size = reads.query("`data size` == 100001")
    plt.plot(reads_data_size["avg len"],
             reads_data_size[available_storage])
    plt.legend(available_storage)
    plt.xlabel("Average string length")
    plt.ylabel("Time required, s")
    plt.gca().set_xscale("log")
    plt.title("Read time vs entry size")
    plt.savefig("read_time_vs_entry_size.png")
    plt.close()

    reads_avg_len = reads.query("`avg len` > 100 and `avg len` < 2000")
    plt.plot(reads_avg_len["data size"],
             reads_avg_len[available_storage])
    plt.legend(available_storage)
    plt.xlabel("Number of entries")
    plt.ylabel("Time required, s")
    plt.title("Read time vs dataset size")
    # plt.gca().set_xscale("log")
    plt.savefig("read_time_vs_dataset_size.png")
    plt.close()


write_fns = {
    "Sqlite3": sqlite3_write,
    # "Shelve": shelve_write,
    "SqliteDict": sqlitedict_write,
    "DbDict": DbDict_write,
    "CompactStorage": compactkvstore_write,
    "KVStore": kvstore_write,
    "DiskCache": diskcache_write,
    "LevelDb": leveldb_write,
    "RocksDB": rocksdb_write,
}


read_fns = {
    "Sqlite3": sqlite3_read,
    # "Shelve": shelve_read,
    "SqliteDict": sqlitedict_read,
    "DbDict": DbDict_read,
    "CompactStorage": compactkvstore_read,
    "KVStore": kvstore_read,
    "DiskCache": diskcache_read,
    "LevelDb": leveldb_read,
    "RocksDB": rocksdb_read,
}


def run_benchmarks():
    text_path = sys.argv[1]
    test_str_lens = [10, 100, 1000, 3000, 6000, 10000]
    test_num_entries = [1e4, 3e4, 6e4, 1e5, 1e6]
    storage_types = ["RocksDB"] #["LevelDb"]# ["Sqlite3", "SqliteDict", "DbDict", "CompactStorage", "KVStore", "DiskCache"]

    # benchmark_writes(text_path, test_str_lens, test_num_entries, storage_types)
    # benchmark_reads(text_path, test_str_lens, test_num_entries, storage_types)
    plot_writes()
    plot_reads()


if __name__ == "__main__":
    run_benchmarks()
