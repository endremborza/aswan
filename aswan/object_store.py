"""Aswan's content-addressed object store, layered over `dirark.BlobStore`.

Workers stage raw bytes (keyed by blake2b checksum) and the main process packs
them into the depot's ark via `commit()` from `AswanDepot.save_current`. The
returned "name" is the blake2b checksum suffixed with a type extension; the
extension drives `read()` deserialization (json/str/bytes/pickle).
"""

import json
import pickle
from pathlib import Path

from dirark import BlobStore


class _Exts:
    txt = "txt"
    pkl = "pkl"
    blob = "blob"
    json = "json"


_READERS = {
    _Exts.blob: "read_bytes",
    _Exts.txt: "read_str",
    _Exts.json: "read_json",
    _Exts.pkl: "read_pickle",
}


class ObjectStore:
    """Content-addressed blob store used by `AswanDepot`.

    Names returned by `dump_*` are `<blake2b_hex>.<ext>` and round-trip through
    the matching `read_*`. The underlying `BlobStore` deduplicates by content.
    """

    def __init__(self, root: Path | str) -> None:
        self.root_path = Path(root)
        self._blobs = BlobStore(self.root_path)

    def purge(self) -> None:
        self._blobs.purge()

    def commit(self) -> None:
        """Pack staged blobs into the ark. Call from main process only."""
        self._blobs.commit()

    def merge_from(self, other: "ObjectStore") -> None:
        self._blobs.merge_from(other._blobs)

    def push(self, remote: str) -> None:
        self._blobs.push(remote)

    def pull(self, remote: str) -> None:
        self._blobs.pull(remote)

    def dump(self, obj: list | dict | str | bytes) -> str:
        if type(obj).__name__ == "BeautifulSoup":
            obj = obj.encode("utf-8")
        if isinstance(obj, (list, dict)):
            return self.dump_json(obj)
        if isinstance(obj, str):
            return self.dump_str(obj)
        if isinstance(obj, bytes):
            return self.dump_bytes(obj)
        return self.dump_pickle(obj)

    def dump_json(self, obj: list | dict) -> str:
        return self.dump_str(json.dumps(obj), _Exts.json)

    def dump_str(self, s: str, ext: str | None = None) -> str:
        return self.dump_bytes(s.encode("utf-8"), ext or _Exts.txt)

    def dump_pickle(self, obj) -> str:
        return self.dump_bytes(pickle.dumps(obj), _Exts.pkl)

    def dump_bytes(self, buf: bytes, ext: str | None = None) -> str:
        checksum = self._blobs.write(buf)
        return f"{checksum}.{ext or _Exts.blob}"

    def read(self, name: str):
        ext = name.rsplit(".", 1)[-1]
        return getattr(self, _READERS[ext])(name)

    def read_json(self, name: str) -> list | dict:
        return json.loads(self.read_str(name))

    def read_str(self, name: str) -> str:
        return self.read_bytes(name).decode("utf-8")

    def read_pickle(self, name: str):
        return pickle.loads(self.read_bytes(name))

    def read_bytes(self, name: str) -> bytes:
        return self._blobs.read(_strip_ext(name))

    def has(self, name: str) -> bool:
        return self._blobs.has(_strip_ext(name))


def _strip_ext(name: str) -> str:
    return name.rsplit(".", 1)[0]
