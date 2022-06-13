import hashlib
import json
import pickle
from pathlib import Path
from typing import Union


class ObjectStore:
    """
    class for storing and retrieving objects downloaded

    :param root: object store root
    """

    def __init__(self, root):
        self.root_path = Path(root)
        self.root_path.mkdir(parents=True, exist_ok=True)

    def purge(self):
        for p in self.root_path.iterdir():
            p.unlink()
        self.root_path.rmdir()

    def dump_json(self, obj: Union[list, dict]) -> str:
        return self.dump_str(json.dumps(obj), "json")

    def dump_str(self, s: str, extension: str = "") -> str:
        return self.dump_bytes(s.encode("utf-8"), extension)

    def dump_pickle(self, obj) -> str:
        return self.dump_bytes(pickle.dumps(obj), ".pkl")

    def dump_bytes(self, buf: bytes, extension: str = "") -> str:
        full_path = self.root_path / f"{hashlib.md5(buf).hexdigest()}.{extension}"
        full_path.write_bytes(buf)
        return full_path.name

    def read_json(self, path: str) -> Union[list, dict]:
        return json.loads(self.read_str(path))

    def read_str(self, path: str) -> str:
        return self.read_bytes(path).decode("utf-8")

    def read_pickle(self, path: str):
        return pickle.loads(self.read_bytes(path))

    def read_bytes(self, path: str) -> bytes:
        return (self.root_path / path).read_bytes()
