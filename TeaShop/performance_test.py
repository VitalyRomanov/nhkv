import sys

from TeaShop import CompactKeyValueStore, DbDict
import shelve
import sqlite3
from sqlitedict import SqliteDict
import pickle as p
from TeaShop import CompactKeyValueStore
import pandas as pd


def test_sqlite3_write(datasize):
    conn = sqlite3.connect(f"debug_{datasize}.s3db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS [mydict] ("
                "[key] VARCHAR(255) PRIMARY KEY NOT NULL, "
                "[value] VARCHAR(255) NOT NULL)")
    for i in range(0, datasize):
        cur.execute("INSERT INTO [mydict] (key, value) VALUES (?, ?)",
                    (str(i), str(i * 2)))
    conn.commit()
    cur.close()
    conn.close()


def test_shelve_write(datasize):
    d = shelve.open(f"debug_{datasize}.shelf", protocol=4)
    for i in range(0, datasize):
        d[str(i)] = str(i * 2)
    d.close()


def test_sqlitedict_write(datasize):
    d = SqliteDict(f'debug_{datasize}.sqlite')
    for i in range(0, datasize):
        d[str(i)] = i * 2
    d.commit()
    d.close()


def test_DbDict_write(datasize):
    d = DbDict(f"debugdb_{datasize}.s3db")
    for i in range(0, datasize):
        d[str(i)] = i * 2
    d.commit()
    d.close()


def test_kvstore_write(datasize):
    d = CompactKeyValueStore(f"debugdb_{datasize}.kv")
    for i in range(0, datasize):
        d[str(i)] = i * 2
    d.commit()
    d.close()
    d.save()


def test_sqlite3_read(datasize):
    conn = sqlite3.connect(f"debug_{datasize}.s3db")
    cur = conn.cursor()
    for i in range(0, datasize):
        cur.execute("select [value] from [mydict] where [key]=?", (str(i),))
        a = cur.fetchall()
    cur.close()
    conn.close()


def test_shelve_read(datasize):
    d = shelve.open(f"debug_{datasize}.shelf", protocol=4)
    for i in range(0, datasize):
        a = d[str(i)]
    d.close()


def test_sqlitedict_read(datasize):
    d = SqliteDict(f'debug_{datasize}.sqlite')
    for i in range(0, datasize):
        a = d[str(i)]
    d.close()


def test_DbDict_read(datasize):
    d = DbDict(f"debugdb_{datasize}.s3db")
    for i in range(0, datasize):
        a = d[str(i)]
    d.close()


def test_kvstore_read(datasize):
    d = CompactKeyValueStore.load(f"debugdb_{datasize}.kv")
    for i in range(0, datasize):
        a = d[str(i)]
    d.close()


import time

text = open(sys.argv[1], "r")

writes = {
    "index": [],
    "sqlite3": [],
    "shelve": [],
    "sqlitedict": [],
    "DbDict": [],
    "KVStore": []
}

for datasize in [20000, 30000, 40000, 50000]:
    writes["index"].append(datasize)

    start_time = time.time()
    test_sqlite3_write(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    writes["sqlite3"].append(emd_time - start_time)
    # writes.append({"Type": "sqlite3", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_shelve_write(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    writes["shelve"].append(emd_time - start_time)
    # writes.append({"Type": "shelve", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_sqlitedict_write(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    writes["sqlitedict"].append(emd_time - start_time)
    # writes.append({"Type": "sqlitedict", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_DbDict_write(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    writes["DbDict"].append(emd_time - start_time)
    # writes.append({"Type": "DbDict", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_kvstore_write(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    writes["KVStore"].append(emd_time - start_time)
    # writes.append({"Type": "KVStore", "datasize": datasize, "time": emd_time - start_time})

writes = pd.DataFrame.from_dict(writes)
writes = writes.set_index("index")
writes.to_csv("writes.csv")

reads = {
    "index": [],
    "sqlite3": [],
    "shelve": [],
    "sqlitedict": [],
    "DbDict": [],
    "KVStore": []
}

for datasize in [50000]:
    reads["index"].append(datasize)

    start_time = time.time()
    test_sqlite3_read(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    reads["sqlite3"].append(emd_time - start_time)
    # reads.append({"Type": "sqlite3", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_shelve_read(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    reads["shelve"].append(emd_time - start_time)
    # reads.append({"Type": "shelve", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_sqlitedict_read(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    reads["sqlitedict"].append(emd_time - start_time)
    # reads.append({"Type": "sqlitedict", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_DbDict_read(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    reads["DbDict"].append(emd_time - start_time)
    # reads.append({"Type": "DbDict", "datasize": datasize, "time": emd_time - start_time})

    start_time = time.time()
    test_kvstore_read(datasize)
    emd_time = time.time()
    print("--- %s seconds ---" % (emd_time - start_time))
    reads["KVStore"].append(emd_time - start_time)
    # reads.append({"Type": "KVStore", "datasize": datasize, "time": emd_time - start_time})

reads = pd.DataFrame.from_dict(reads)
reads = reads.set_index("index")
reads.to_csv("reads.csv")