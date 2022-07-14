import pytest

from aswan.object_store import ObjectStore


@pytest.fixture
def tmp_obj_store(tmp_path):
    return ObjectStore(tmp_path)


def test_obj_store(tmp_obj_store: ObjectStore):

    test_obj = {"X": 10}

    json_path = tmp_obj_store.dump_json(test_obj)
    str_path = tmp_obj_store.dump_str(str(test_obj))
    buf_path = tmp_obj_store.dump_bytes(str(test_obj).encode("utf-8"))
    pkl_path = tmp_obj_store.dump_pickle(test_obj)
    assert test_obj == tmp_obj_store.read_json(json_path)
    assert str(test_obj) == tmp_obj_store.read_str(str_path)
    assert str(test_obj).encode("utf-8") == tmp_obj_store.read_bytes(buf_path)
    assert test_obj == tmp_obj_store.read_pickle(pkl_path)


def test_obj_store_pathgen(tmp_obj_store: ObjectStore):

    obj = {"A": 2}
    opath = tmp_obj_store.dump_pickle(obj)
    assert obj == tmp_obj_store.read_pickle(opath)
    assert obj == tmp_obj_store.read_pickle(tmp_obj_store.root_path / opath)


def test_purge(tmp_obj_store: ObjectStore):
    objs = [{"A": 3}, ["X", 20, ["C"]]]
    opaths = []
    for obj in objs:
        opath = tmp_obj_store.dump_pickle(obj)
        opaths.append(opath)
        assert obj == tmp_obj_store.read_pickle(opath)
    tmp_obj_store.purge()
    for obj, opath in zip(objs, opaths):
        with pytest.raises(FileNotFoundError):
            assert obj == tmp_obj_store.read_pickle(opath)
