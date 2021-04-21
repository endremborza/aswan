import hashlib
import json
import os
import pickle
import tarfile
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Optional, Union

import s3fs


class ObjectStoreBase(ABC):
    """
    class for storing and retrieving objects downloaded

    :param root: object store root
    """

    def __init__(self, root):
        self._root = root

    @abstractmethod
    def open(self, path, method):
        pass

    @abstractmethod
    def purge(self):
        pass

    @abstractmethod
    def _local_iter(self):
        pass

    def dump_json(
        self, obj: Union[list, dict], path: Optional[str] = None
    ) -> str:
        return self.dump_str(json.dumps(obj), path, ".json")

    def dump_str(
        self, s: str, path: Optional[str] = None, extension: str = ""
    ) -> str:
        return self.dump_bytes(s.encode("utf-8"), path, extension)

    def dump_pickle(self, obj, path: Optional[str] = None) -> str:
        return self.dump_bytes(pickle.dumps(obj), path, ".pkl")

    def dump_bytes(
        self, buf: bytes, path: Optional[str] = None, extension: str = ""
    ) -> str:
        full_path = self._get_abs_path(path, buf, extension)
        with self.open(full_path, "wb") as fp:
            fp.write(buf)
        return full_path.replace(self._root + "/", "", 1)

    def read_json(self, path: str) -> Union[list, dict]:
        return json.loads(self.read_str(path))

    def read_str(self, path: str) -> str:
        return self.read_bytes(path).decode("utf-8")

    def read_pickle(self, path: str):
        return pickle.loads(self.read_bytes(path))

    def read_bytes(self, path: str) -> bytes:
        full_path = self._get_abs_path(path)
        with self.open(full_path, "rb") as fp:
            return fp.read()

    def _get_abs_path(
        self,
        path: Optional[str],
        buf: Optional[bytes] = None,
        extension: str = "",
    ) -> str:
        if path is None:
            path = hashlib.md5(buf).hexdigest() + extension

        if path.startswith(self._root):
            return path
        return f"{self._root}/{path}"

    @contextmanager
    def tarcontext(self):
        with tempfile.NamedTemporaryFile() as tfp:
            tarpath = tfp.name
            with tarfile.open(tarpath, "w:gz") as tar:
                for local_abs_path, name in self._local_iter():
                    tar.add(local_abs_path, arcname=name)
            yield tarpath


class LocalObjectStore(ObjectStoreBase):
    def open(self, path, method):
        return open(path, method)

    def purge(self):
        for p, _ in self._local_iter():
            os.unlink(p)

    def _local_iter(self):
        for p in os.listdir(self._root):
            yield (os.path.join(self._root, p), p)


class S3ObjectStore(ObjectStoreBase):
    def __init__(self, root: str):
        super().__init__(root)
        self._fs = s3fs.S3FileSystem(anon=False)

    def open(self, path, method):
        return self._fs.open(path, method)

    def purge(self):
        for p in self._fs.ls(self._root):
            self._fs.delete(p)

    def _local_iter(self):
        for p in self._fs.ls(self._root):
            with tempfile.NamedTemporaryFile() as tfp:
                tfp.write(self.read_bytes(p))
                yield (tfp.name, p)


def get_object_store(root: str):
    if root.startswith("s3://"):
        return S3ObjectStore(root)
    return LocalObjectStore(root)
