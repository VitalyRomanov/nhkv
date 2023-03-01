import pickle
from array import array


class CompactStorage:
    """
    CompactStorage is a class that stores n fields of the same data type as array instead of python
    objects. Such representation allows to save a considerable amount of space and retrieval time. During
    reads, creates a memoryview that allows to further reduce reading time. Keys must be added sequentially.
    Meant for internal use.
    """
    def __init__(self, n_fields=1, dtype="L"):
        """
        Creates a CompactStorage objects with n fields of type dtype
        :param n_fields: Number of field per record. Records are returned as tuples.
        :param dtype: Type of fields. All fields have the same type. The type descriptor should be one
        of available in standard array package.
        """
        self._storage = array(dtype)
        self._view = None
        self._n_fields = n_fields
        self._active_storage_size = 0
        self._has_view = False

    def _create_view(self):
        if self._has_view is False:
            self._view = memoryview(self._storage)
            self._has_view = True

    def _release_view(self):
        if self._has_view is True:
            self._view.release()
            self._view = None
            self._has_view = False

    def _get_array_span_for_item(self, item):
        offset = item * self._n_fields
        if offset < 0:
            offset += len(self) * self._n_fields
        return offset, offset + self._n_fields

    def __len__(self):
        return self._active_storage_size

    def __getitem__(self, item):
        if item >= len(self):
            raise IndexError("Out of range:", item)

        self._create_view()

        start, end = self._get_array_span_for_item(item)

        return tuple(self._view[start: end].tolist())

    def __setitem__(self, item, value):
        """
        Overwrite existing key.
        :param item: Integer index
        :param value: tuple of length `n_fields`
        :return:
        """

        self._release_view()

        if item >= len(self):
            raise IndexError("Out of range:", item)

        start, _ = self._get_array_span_for_item(item)

        for ind, v in enumerate(value):
            self._storage[start + ind] = v

    def append(self, value):
        """
        Append new entry.
        :param value: tuple of length `n_fields`
        :return: index of added entry
        """
        self._release_view()

        self._storage.extend(value)
        self._active_storage_size += 1
        return self._active_storage_size - 1

    def save(self, path):
        self._release_view()
        pickle.dump(self, open(path, "wb"), protocol=4)

    @classmethod
    def load(cls, path):
        return pickle.load(open(path, "rb"))
