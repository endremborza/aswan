import datetime as dt
from dataclasses import asdict, dataclass, field
from functools import total_ordering
from hashlib import md5
from pathlib import Path
from typing import Callable, Dict, Iterable, Type, Union

import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

BLOB_JOIN = b"\n"
NAME_JOIN = "-"


class SourceUrl(Base):
    __tablename__ = "source_urls"

    cid = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String)
    handler = db.Column(db.String)
    current_status = db.Column(db.String)
    uix = db.UniqueConstraint(url, handler)

    def __repr__(self):
        return f"SourceURL: {self.handler}: {self.url} - {self.current_status}"


@dataclass
class _Event:
    url: str
    handler: str
    _read_fun: Callable[..., bytes] = field(default=None, init=False)
    _extended: bool = field(default=False, init=False)

    __name_keys__ = ["handler"]
    __name_prefix__ = ""

    def dump(self, dir_path: Path):
        name_parts = map(_to_str, [getattr(self, k) for k in self.__name_keys__])
        blob_parts = map(_to_bytes, [getattr(self, k) for k in self._blob_keys()])
        blob = BLOB_JOIN.join(blob_parts)
        filename = NAME_JOIN.join(
            [self.__name_prefix__, *name_parts, md5(blob).hexdigest()]
        )
        (dir_path / filename).write_bytes(blob)
        return self

    def extend(self):
        if self._extended:
            return self
        self._extended = True
        blob = self._read_fun()
        for k, v in zip(self._blob_keys(), blob.split(BLOB_JOIN)):
            setattr(self, k, _from_bytes(v, self._ann()[k]))
        return self

    def dict(self):
        return {k: v for k, v in asdict(self.extend()).items() if k in self._ann()}

    @classmethod
    def partial_load(cls, name: str, blob_loader: Callable):
        main = {}
        for k, val in zip(cls.__name_keys__, name.split(NAME_JOIN)[1:-1]):
            main[k] = _from_str(val, cls._ann()[k])
        out = cls(**main, **{k: None for k in cls._blob_keys()})
        out._read_fun = blob_loader
        return out

    @classmethod
    def _blob_keys(cls):
        return filter(lambda k: k not in cls.__name_keys__, cls._ann().keys())

    @classmethod
    def _ann(cls) -> Dict[str, type]:
        _full_ann_items = {**cls.__annotations__, **_Event.__annotations__}.items()
        return {k: v for k, v in _full_ann_items if not k.startswith("_")}


@dataclass
class RegEvent(_Event):
    overwrite: bool = False

    __name_prefix__ = "r"


@total_ordering
@dataclass
class CollEvent(_Event):
    status: str
    timestamp: int
    output_file: str  # empty if no output

    __name_keys__ = ["handler", "timestamp", "status"]
    __name_prefix__ = "c"

    def __repr__(self):
        return f"----\n{self.iso}\n{self.status}\n{self.url}\n{self.handler}\n----"

    def __eq__(self, __o: "CollEvent"):
        return self.timestamp == __o.timestamp

    def __le__(self, __o: "CollEvent"):
        # less if more recent
        return self.timestamp >= __o.timestamp

    @property
    def iso(self):
        return dt.datetime.fromtimestamp(self.timestamp).isoformat()


def partial_read_path(file_path: Path) -> Union[CollEvent, RegEvent]:
    return partial_read(file_path.name, file_path.read_bytes)


def partial_read(file_name: str, reader: Callable) -> Union[CollEvent, RegEvent]:
    event_types: Iterable[Type[_Event]] = [CollEvent, RegEvent]
    for ev_type in event_types:
        if file_name.startswith(ev_type.__name_prefix__):
            return ev_type.partial_load(file_name, reader)


def _to_bytes(val):
    return _to_str(val).encode("utf-8")


def _to_str(val):
    if isinstance(val, bool):
        return "T" if val else "F"
    if isinstance(val, int):
        return hex(val)[2:]
    assert isinstance(val, str), f"is {type(val)} - {val}"
    return val


def _from_str(val: str, dtype):
    if dtype == int:
        return int(val, base=16)
    if dtype == bool:
        return val == "T"
    assert isinstance(val, str)
    return val


def _from_bytes(val: bytes, dtype):
    return _from_str(val.decode("utf-8"), dtype)
