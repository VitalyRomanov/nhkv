from pathlib import Path

from nhkv.DbDict import DbDict
from nhkv.KVStore import KVStore, CompactKeyValueStore


class _ContextManager:
    storage_instances = {}

    def __init__(self):
        raise Exception("Do not initialize this class")

    @classmethod
    def get_instance(cls, path):
        return cls.storage_instances.get(path, None)

    @classmethod
    def set_instance(cls, path, instance):
        cls.storage_instances[path] = instance


def get_or_create_storage(storage_class, **kwargs):
    path = kwargs.get("path", None)
    if path is None:
        raise ValueError("Path is not specified")

    if isinstance(path, Path):
        pass
    elif isinstance(path, str):
        path = Path(path)
    else:
        raise ValueError(f"Path type is not recognized: {type(path)}")

    path = str(path.absolute())

    instance = _ContextManager.get_instance(path)
    if instance is not None:
        assert storage_class == type(instance), "Types of requested and existing storage do not match"
    else:
        instance = storage_class(**kwargs)
        _ContextManager.set_instance(path, instance)

    return instance
