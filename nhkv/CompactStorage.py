from array import array
from time import time


class CompactStorage:
    def __init__(self, n_fields=1, dtype="L"):
        self._storage = array(dtype)
        self._view = None
        self._n_fields = n_fields
        self._active_storage_size = 0
        self._has_view = False

    def __len__(self):
        return self._active_storage_size

    def keys(self):
        return list(range(len(self._storage) // self._n_fields))

    def _create_view(self):
        if self._has_view is False:
            self._view = memoryview(self._storage)
            self._has_view = True

    def _release_view(self):
        if self._has_view is True:
            self._view.release()
            self._has_view = False

    def __getitem__(self, item):
        if item >= len(self):
            raise IndexError("Out of range:", item)

        self._create_view()

        offset = item * self._n_fields

        return tuple(self._view[offset: offset + self._n_fields].tolist())

    def __setitem__(self, key, value):

        self._release_view()

        if key >= len(self):
            raise IndexError("Out of range:", key)

        key = key * self._n_fields

        for ind, v in enumerate(value):
            self._storage[key + ind] = v

    def append(self, value):
        self._release_view()

        self._storage.extend(value)
        self._active_storage_size += 1


def test_CompactStorage():
    import random

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

    start = time()
    retrieved = []
    for ind in to_get:
        retrieved.append(s1[ind])
    end = time()

    print(f"S1 read duration {end - start} seconds")

    assert proper == retrieved



# # def compare_speed():
# #     import random
# #
# #     random_range = list(range(100000))
# #
# #     num_to_write = 10000000
# #
# #     to_write = list(zip(random.choices(random_range, k=num_to_write), random.choices(random_range, k=num_to_write)))
# #     to_get = list(range(num_to_write))
# #     random.shuffle(to_get)
# #
# #     s1 = CompactStorage(2)
# #     s2 = CompactStorage2(2)
# #     s3 = CompactStorage3()
# #     s4 = CompactStorage4(2)
# #
# #     print("starting tests")
# #
# #     start = time()
# #     for record in to_write:
# #         s1.append(record)
# #     end = time()
# #
# #     print(f"S1 write duration {end - start} seconds")
# #
# #     start = time()
# #     for ind in to_get:
# #         a = s1[ind]
# #     end = time()
# #
# #     print(f"S1 read duration {end - start} seconds")
# #
# #
# #
# #     start = time()
# #     for record in to_write:
# #         s2.append(record)
# #     end = time()
# #
# #     print(f"S2 write duration {end - start} seconds")
# #
# #     start = time()
# #     for ind in to_get:
# #         a = s2[ind]
# #     end = time()
# #
# #     print(f"S2 read duration {end - start} seconds")
# #
# #
# #
# #
# #     start = time()
# #     for record in to_write:
# #         s3.append(record)
# #     end = time()
# #
# #     print(f"S3 write duration {end - start} seconds")
# #
# #     start = time()
# #     for ind in to_get:
# #         a = s3[ind]
# #     end = time()
# #
# #     print(f"S3 read duration {end - start} seconds")
# #
# #
# #
# #     start = time()
# #     for record in to_write:
# #         s4.append(record)
# #     end = time()
# #
# #     print(f"S4 write duration {end - start} seconds")
# #
# #     s4.view = memoryview(s4._storage)
# #
# #     start = time()
# #     for ind in to_get:
# #         a = s4[ind]
# #     end = time()
# #
# #     print(f"S4 read duration {end - start} seconds")
#
#
# if __name__ == "__main__":
#     compare_speed()