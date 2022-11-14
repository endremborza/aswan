import hashlib
import json
import pickle
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Union

from bs4 import BeautifulSoup

_COMP_NAME = "content"


class _Exts:

    txt = "txt"
    pkl = "pkl"
    blob = "blob"
    json = "json"


class ObjectStore:
    """
    class for storing and retrieving objects downloaded

    :param root: object store root
    """

    def __init__(
        self,
        root,
        hash_fun=hashlib.sha3_512,
        prefix_chars: int = 2,
        compression=zipfile.ZIP_DEFLATED,
        timeout=60,
    ):
        self.root_path = Path(root)
        self.hash_fun = hash_fun
        self.prefix_chars = prefix_chars
        self.timeout = timeout
        self._comp = compression

    def purge(self, clear_dirs=True):
        for _dir in self.root_path.iterdir():
            for p in _dir.iterdir():
                p.unlink()
            _dir.rmdir()
        if clear_dirs:
            self.root_path.rmdir()

    def dump(self, obj: Union[list, dict, str, bytes]):
        if isinstance(obj, BeautifulSoup):
            # can result in infinite recursion for pickling, dunno why
            obj = obj.encode("utf-8")
        for _t, fun in [
            ((list, dict), self.dump_json),
            (str, self.dump_str),
            (bytes, self.dump_bytes),
        ]:
            if isinstance(obj, _t):
                return fun(obj)
        return self.dump_pickle(obj)

    def dump_json(self, obj: Union[list, dict]) -> str:
        return self.dump_str(json.dumps(obj), _Exts.json)

    def dump_str(self, s: str, ext=None) -> str:
        return self.dump_bytes(s.encode("utf-8"), ext or _Exts.txt)

    def dump_pickle(self, obj) -> str:
        return self.dump_bytes(pickle.dumps(obj), _Exts.pkl)

    def dump_bytes(self, buf: bytes, ext=None) -> str:
        name_hash = self.hash_fun(buf).hexdigest()
        full_name = _join(".", name_hash, ext or _Exts.blob)
        full_path = self._get_full_path(full_name)
        if full_path.exists():
            return full_name
        full_path.parent.mkdir(exist_ok=True, parents=True)
        with self._zip(full_path, "w") as zip_ctx:
            zip_ctx.writestr(_COMP_NAME, buf)

        return full_name

    def read(self, name: str):
        _ext = name.split(".")[-1]
        return {
            _Exts.blob: self.read_bytes,
            _Exts.json: self.read_json,
            _Exts.txt: self.read_str,
            _Exts.pkl: self.read_pickle,
        }[_ext](name)

    def read_json(self, name: str) -> Union[list, dict]:
        return json.loads(self.read_str(name))

    def read_str(self, name: str) -> str:
        return self.read_bytes(name).decode("utf-8")

    def read_pickle(self, name: str):
        return pickle.loads(self.read_bytes(name))

    def read_bytes(self, name: str) -> bytes:
        with self._zip(self._get_full_path(name), "r") as zip_ctx:
            return zip_ctx.read(_COMP_NAME)

    def _get_full_path(self, full_name: str) -> Path:
        dirname = full_name[: self.prefix_chars]
        return self.root_path / dirname / full_name

    @contextmanager
    def _zip(self, full_path, mode) -> Generator[zipfile.ZipFile, None, None]:
        with zipfile.ZipFile(full_path, mode, compression=self._comp) as zip_ctx:
            yield zip_ctx


def _join(sep: str, *elems):
    return sep.join(filter(None, elems))
