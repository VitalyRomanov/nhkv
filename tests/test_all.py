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

    assert Path("temp").is_dir() is False

    storage = KVStore("temp", index_backend="sqlite")
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

    storage = KVStore.load("temp")
    assert len(storage) == 3
    assert storage.keys() == [0, 10, 12]
    assert storage[10] == ["size"] * 5
    storage.close()

    del storage

    import shutil
    shutil.rmtree("temp")


def test_kv_store_shelve():
    from pathlib import Path
    from nhkv.KVStore import KVStore

    assert Path("temp").is_dir() is False

    storage = KVStore("temp", index_backend="shelve")
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

    storage._flush_shards()
    storage.save()
    storage["12"] = "tonight"
    storage.close()
    del storage

    path = Path("temp")
    print(list(path.iterdir()))
    print(path.absolute())

    storage = KVStore.load("temp")
    assert len(storage) == 3
    assert storage.keys() == ["0", "10", "12"]
    assert storage["10"] == ["size"] * 5
    storage.close()

    del storage

    import shutil
    shutil.rmtree("temp")
