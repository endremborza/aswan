from random import choice, choices
from string import ascii_letters

import pytest
from atqo import parallel_map
from bs4 import BeautifulSoup

from aswan.object_store import ObjectStore


@pytest.fixture
def tmp_obj_store(tmp_path):
    return ObjectStore(tmp_path)


def test_obj_store(tmp_obj_store: ObjectStore):

    test_obj = {"X": 10}
    test_pkl_obj = ("F", False)

    json_path = tmp_obj_store.dump_json(test_obj)
    assert json_path == tmp_obj_store.dump(test_obj)
    str_path = tmp_obj_store.dump_str(str(test_obj))
    buf_path = tmp_obj_store.dump_bytes(str(test_obj).encode("utf-8"))
    pkl_path = tmp_obj_store.dump_pickle(test_obj)
    pkl_path2 = tmp_obj_store.dump(test_pkl_obj)
    assert test_obj == tmp_obj_store.read_json(json_path)
    assert str(test_obj) == tmp_obj_store.read_str(str_path)
    assert str(test_obj).encode("utf-8") == tmp_obj_store.read_bytes(buf_path)
    assert test_obj == tmp_obj_store.read_pickle(pkl_path)
    assert test_pkl_obj == tmp_obj_store.read(pkl_path2)


def test_obj_store_pathgen(tmp_obj_store: ObjectStore):

    obj = {"A": 2}
    oname = tmp_obj_store.dump_pickle(obj)
    assert obj == tmp_obj_store.read_pickle(oname)


def test_multi_objects(tmp_path, tmp_obj_store: ObjectStore):
    objects = [
        {choice(ascii_letters): "".join(choices(ascii_letters, k=40) * 4)}
        for _ in range(25)
    ]
    r2 = tmp_path / "os2"
    r2.mkdir()
    os2 = ObjectStore(r2)
    para_names = list(parallel_map(os2.dump_json, objects))
    sync_names = [tmp_obj_store.dump_json(o) for o in objects]
    for name, obj in zip(sync_names, objects):
        assert obj == tmp_obj_store.read_json(name)
        assert name in para_names
        assert obj == os2.read_json(name)


def test_purge(tmp_obj_store: ObjectStore):
    objs = [{"A": 3}, ["X", 20, ["C"]]]
    onames = []
    for obj in objs:
        oname = tmp_obj_store.dump_pickle(obj)
        onames.append(oname)
        assert obj == tmp_obj_store.read_pickle(oname)
    tmp_obj_store.purge()
    for obj, oname in zip(objs, onames):
        with pytest.raises(FileNotFoundError):
            assert obj == tmp_obj_store.read_pickle(oname)


def test_soup(tmp_obj_store: ObjectStore):
    s = BeautifulSoup("<html></html>", "html5lib")
    tmp_obj_store.dump(s)
