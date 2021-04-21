import os

import pytest

from aswan.object_store import get_object_store


def test_obj_store(tmp_path):

    obst = get_object_store(str(tmp_path))

    test_obj = {"X": 10}

    json_path = "test.json"
    str_path = "test.txt"
    buf_path = "testfile"
    pkl_path = "testfile.pkl"

    obst.dump_json(test_obj, json_path)
    obst.dump_str(str(test_obj), str_path)
    obst.dump_bytes(str(test_obj).encode("utf-8"), buf_path)
    obst.dump_pickle(test_obj, pkl_path)
    assert test_obj == obst.read_json(json_path)
    assert str(test_obj) == obst.read_str(str_path)
    assert str(test_obj).encode("utf-8") == obst.read_bytes(buf_path)
    assert test_obj == obst.read_pickle(pkl_path)


def test_obj_store_pathgen(tmp_path):

    obst = get_object_store(str(tmp_path))
    obj = {"A": 2}
    opath = obst.dump_pickle(obj)
    assert obj == obst.read_pickle(opath)
    assert obj == obst.read_pickle(os.path.join(tmp_path, opath))


def test_purge(tmp_path):
    obst = get_object_store(str(tmp_path))

    objs = [{"A": 3}, ["X", 20, ["C"]]]
    opaths = []
    for obj in objs:
        opath = obst.dump_pickle(obj)
        opaths.append(opath)
        assert obj == obst.read_pickle(opath)
    obst.purge()
    for obj, opath in zip(objs, opaths):
        with pytest.raises(FileNotFoundError):
            assert obj == obst.read_pickle(opath)
