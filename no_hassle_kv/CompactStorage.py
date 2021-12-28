from array import array
from time import time


# class CompactStorage:
#     def __init__(self, n_fields=1, init_size=100000, dtype=np.uint32, volatile_access=False):
#         if n_fields == 0:
#             raise ValueError("The parameter n_fields should be greater than 0")
#
#         self.volatile_access = volatile_access
#
#         if n_fields == 1:
#             self.storage = np.zeros(shape=(init_size, ), dtype=dtype)
#         else:
#             self.storage = np.zeros(shape=(init_size, n_fields), dtype=dtype)
#
#         self.active_storage_size = 0
#
#     def resize_storage(self, new_size):
#         if len(self.storage.shape) == 1:
#             self.storage.resize((new_size,))
#         else:
#             self.storage.resize((new_size, self.storage.shape[1]))
#
#     def __len__(self):
#         if self.volatile_access:
#             return self.storage.shape[0]
#         else:
#             return self.active_storage_size
#
#     def __getitem__(self, item):
#         if item >= len(self):
#             raise IndexError("Out of range:", item)
#
#         if len(self.storage.shape) == 1:
#             return self.storage[item]
#         else:
#             return tuple(self.storage[item])
#
#     def __setitem__(self, key, value):
#         # if self.volatile_access:
#         #     if key >= len(self):
#         #         self.resize_storage(int(key*1.2))
#
#         if key >= len(self):
#             # if self.volatile_access:
#             #     raise IndexError("Out of range:", key, "Preallocate when using volatile_access=True")
#             raise IndexError("Out of range:", key)
#
#         if len(self.storage.shape) == 1:
#             self.storage[key] = value
#         else:
#             self.storage[key, :] = np.fromiter(value, dtype=self.storage.dtype)
#
#     def append(self, value):
#         if self.volatile_access:
#             raise Exception("Use __setitem__ when volatile_access=True")
#
#         if self.active_storage_size >= self.storage.shape[0]:
#             self.resize_storage(int(self.storage.shape[0]*1.2))
#
#         if len(self.storage.shape) == 1:
#             self.storage[self.active_storage_size] = value
#         else:
#             self.storage[self.active_storage_size, :] = np.fromiter(value, dtype=self.storage.dtype)
#
#         self.active_storage_size += 1
#
#
# class CompactStorage2:
#     def __init__(self, n_fields=1, dtype="L"):
#         if n_fields == 0:
#             raise ValueError("The parameter n_fields should be greater than 0")
#
#         self.storage = [array(dtype) for _ in range(n_fields)]
#
#         self.active_storage_size = 0
#
#     def __len__(self):
#         return self.active_storage_size
#
#     def __getitem__(self, item):
#         if item >= len(self):
#             raise IndexError("Out of range:", item)
#
#         return tuple(field[item] for field in self.storage)
#
#     def __setitem__(self, key, value):
#
#         if key >= len(self):
#             raise IndexError("Out of range:", key)
#
#         for v, field in zip(value, self.storage):
#             field[key] = value
#
#     def append(self, value):
#         for v, field in zip(value, self.storage):
#             field.append(v)
#         self.active_storage_size += 1
#
#
# class CompactStorage3:
#     def __init__(self):
#         self.storage = array("Q")
#         self.active_storage_size = 0
#
#     def __len__(self):
#         return self.active_storage_size
#
#     def __getitem__(self, item):
#         if item >= len(self):
#             raise IndexError("Out of range:", item)
#
#         return self.unpack(self.storage[item])
#
#     def pack(self, range):
#         return struct.unpack("=Q", struct.pack("=LL", *range))[0]
#
#     def unpack(self, value):
#         return struct.unpack("=LL", struct.pack("=Q", value))
#
#     def __setitem__(self, key, value):
#
#         if key >= len(self):
#             raise IndexError("Out of range:", key)
#
#         for v, field in zip(value, self.storage):
#             field[key] = self.pack(value)
#
#     def append(self, value):
#         self.storage.append(self.pack(value))
#         self.active_storage_size += 1


class CompactStorage:
    def __init__(self, n_fields=1, dtype="L"):
        self.storage = array(dtype)
        self.view = None
        self.n_fields = n_fields
        self.active_storage_size = 0
        self.has_view = False

    def __len__(self):
        return self.active_storage_size

    def _create_view(self):
        if self.has_view is False:
            self.view = memoryview(self.storage)
            self.has_view = True

    def _release_view(self):
        if self.has_view is True:
            self.view.release()
            self.has_view = False

    def __getitem__(self, item):
        if item >= len(self):
            raise IndexError("Out of range:", item)

        self._create_view()

        offset = item * self.n_fields

        return tuple(self.view[offset: offset + self.n_fields].tolist())

    def __setitem__(self, key, value):

        self._release_view()

        if key >= len(self):
            raise IndexError("Out of range:", key)

        key = key * self.n_fields

        for ind, v in enumerate(value):
            self.storage[key + ind] = v

    def append(self, value):
        self._release_view()

        self.storage.extend(value)
        self.active_storage_size += 1


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
# #     s4.view = memoryview(s4.storage)
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