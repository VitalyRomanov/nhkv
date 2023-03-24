import logging
import pickle
from abc import abstractmethod, ABC


class AbstractDbDict(ABC):
    _is_open = False
    requires_commit = False

    def __init__(self, path, serializer=None, deserializer=None, **kwargs):
        self.path = path
        self._initialize_connection(path, **kwargs)
        self._init_serializers(serializer, deserializer)
        self._is_open = True
        self.requires_commit = False

    @abstractmethod
    def _initialize_connection(self, path, **kwargs):
        ...

    def _init_serializers(self, serializer, deserializer):
        if serializer is not None and deserializer is not None:
            self._serialize = serializer
            self._deserialize = deserializer
            return

        if (
                serializer is not None and deserializer is None or
                serializer is None and deserializer is not None
        ):
            logging.warning("Both, serializer and deserializer should be specified. Fallback to pickle.")

        self._serialize = lambda value: pickle.dumps(value, protocol=4, fix_imports=False)
        self._deserialize = lambda value: pickle.loads(value)

    @abstractmethod
    def _check_key_type(self, key):
        ...

    @abstractmethod
    def __setitem__(self, key, value):
        ...

    @abstractmethod
    def __getitem__(self, key):
        ...

    def __delitem__(self, key):
        ...

    @abstractmethod
    def __len__(self):
        ...

    def __del__(self):
        self.close()

    def get(self, item, default):
        try:
            return self[item]
        except KeyError:
            return default

    @abstractmethod
    def keys(self):
        ...

    @abstractmethod
    def save(self):
        ...

    @abstractmethod
    def close(self):
        ...
