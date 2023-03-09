import os
import shutil


def test_compact_storage():
    import random
    from nhkv.CompactStorage import CompactStorage
    from time import time

    random_range = list(range(1000))

    num_to_write = 100

    to_write = list(zip(random.choices(random_range, k=num_to_write), random.choices(random_range, k=num_to_write)))
    to_get = list(range(num_to_write))
    random.shuffle(to_get)
    proper = [to_write[ind] for ind in to_get]

    s1 = CompactStorage(2)

    print("starting tests")

    start = time()
    for record in to_write:
        s1.append(record)
    end = time()

    print(f"S1 write duration {end - start} seconds")
    assert len(s1) == num_to_write

    start = time()
    retrieved = []
    for ind in to_get:
        retrieved.append(s1[ind])
    end = time()

    print(f"S1 read duration {end - start} seconds")

    assert proper == retrieved

    assert s1[-1] == s1[len(s1) - 1]

    s1.save("test_save.pkl")

    s2 = CompactStorage.load("test_save.pkl")
    assert s2[-1] == s2[len(s2) - 1]

    import os

    os.remove("test_save.pkl")


def test_db_offset_storage():
    from nhkv.DbOffsetStorage import DbOffsetStorage

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


def test_db_dict():
    from nhkv.DbDict import DbDict
    import os

    int_db_path = "test_int_key.db"
    storage = DbDict(int_db_path, key_type=int)
    storage[1] = 2
    storage[0] = 1
    storage[3] = 4
    try:
        storage["cat"] = 4
        assert False, "Exception is not caught"
    except TypeError:
        pass

    try:
        test = storage[2]
        assert False, "Exception is not caught"
    except KeyError:
        pass

    try:
        test = storage["cat"]
        assert False, "Exception is not caught"
    except TypeError:
        pass

    assert len(storage) == 3

    assert storage.keys() == [0, 1, 3]

    storage.save()
    test = storage[1]
    storage.close()  # not reversible
    del storage
    os.remove(int_db_path)

    str_db_path = "test_str_key.db"
    storage = DbDict(str_db_path, key_type=str)
    storage["1"] = 2
    storage["0"] = 1
    storage["3"] = 4

    try:
        storage[3] = 4
        assert False, "Exception is not caught"
    except TypeError:
        pass

    try:
        test = storage["2"]
        assert False, "Exception is not caught"
    except KeyError:
        pass

    try:
        test = storage[3]
        assert False, "Exception is not caught"
    except TypeError:
        pass

    assert len(storage) == 3

    assert storage.keys() == ["1", "0", "3"]

    storage.save()
    test = storage["1"]
    storage.close()  # not reversible
    del storage
    os.remove(str_db_path)


def test_compact_key_value_storage():
    from nhkv.KVStore import CompactKeyValueStore

    storage = CompactKeyValueStore("temp")
    storage[0] = 1
    storage["nice"] = ["nice"] * 10
    storage["nice"] = ["nice"] * 5
    storage["nice"] = ["size"] * 5

    try:
        test = storage["not nice"]
        assert False, "Exception is not caught"
    except KeyError:
        pass

    assert len(storage) == 2
    assert storage.keys() == [0, "nice"]

    storage.save()
    storage["later"] = "tonight"
    storage.close()
    del storage

    storage = CompactKeyValueStore.load("temp")
    assert len(storage) == 3
    assert storage.keys() == [0, "nice", "later"]
    assert storage["nice"] == ["size"] * 5
    storage.close()

    del storage

    import shutil
    shutil.rmtree("temp")


def test_kv_store_sqlite():
    from pathlib import Path
    from nhkv.KVStore import KVStore

    path = "temp_sqlite"
    assert Path(path).is_dir() is False

    storage = KVStore(path, index_backend="sqlite")
    storage[0] = 1
    storage[10] = ["nice"] * 10
    storage[10] = ["nice"] * 5
    storage[10] = ["size"] * 5

    try:
        test = storage[11]
        assert False, "Exception is not caught"
    except KeyError:
        pass

    try:
        storage["12"] = None
        assert False, "Exception is not caught"
    except TypeError:
        pass

    try:
        test = storage["12"]
        assert False, "Exception is not caught"
    except TypeError:
        pass

    assert len(storage) == 2
    assert storage.keys() == [0, 10]

    storage._flush_shards()
    storage.save()
    storage[12] = "tonight"
    storage.close()
    del storage

    storage = KVStore.load(path)
    assert len(storage) == 3
    assert storage.keys() == [0, 10, 12]
    assert storage[10] == ["size"] * 5
    storage.close()

    del storage

    import shutil
    shutil.rmtree(path)


def test_kv_store_shelve():
    from pathlib import Path
    from nhkv.KVStore import KVStore

    path = "temp_shelve"

    assert Path(path).is_dir() is False

    storage = KVStore(path, index_backend="shelve")
    storage["0"] = 1
    storage["10"] = ["nice"] * 10
    storage["10"] = ["nice"] * 5
    storage["10"] = ["size"] * 5

    try:
        test = storage["11"]
        assert False, "Exception is not caught"
    except KeyError:
        pass

    try:
        storage[12] = None
        assert False, "Exception is not caught"
    except TypeError:
        pass

    try:
        test = storage[12]
        assert False, "Exception is not caught"
    except TypeError:
        pass

    assert len(storage) == 2
    assert storage.keys() == ["0", "10"]

    # storage._flush_shards()
    storage.save()
    storage["12"] = "tonight"
    storage.close()
    del storage

    storage = KVStore.load(path)
    assert len(storage) == 3
    assert storage.keys() == ["0", "10", "12"]
    assert storage["10"] == ["size"] * 5
    storage.close()

    del storage

    import shutil
    shutil.rmtree(path)


def test_context_manager():
    from nhkv import get_or_create_storage
    from nhkv import DbDict
    dbdict_path = "dbdict.db"
    storage1 = get_or_create_storage(DbDict, path=dbdict_path)
    storage2 = get_or_create_storage(DbDict, path=dbdict_path)
    assert id(storage1) == id(storage2)

    from nhkv import KVStore
    storage_path = "storage"
    storage1 = get_or_create_storage(KVStore, path=storage_path)
    storage2 = get_or_create_storage(KVStore, path=storage_path)
    assert id(storage1) == id(storage2)

    os.remove(dbdict_path)
    shutil.rmtree(storage_path)


def test_storage_lock():
    from nhkv import get_or_create_storage
    from nhkv import KVStore
    from pathlib import Path

    storage_path = Path("storage")
    storage1 = get_or_create_storage(KVStore, path=storage_path)
    storage1[0] = "test"
    assert storage_path.joinpath("lock").is_file()
    test = storage1[0]
    assert not storage_path.joinpath("lock").is_file()
    storage1[0] = "test"
    storage1.save()
    shutil.rmtree(storage_path)
