from functools import partial

from aswan.constants import Statuses
from aswan.metadata_handling import add_urls, get_next_batch
from aswan.models import CollEvent, SourceUrl

get_surl = partial(
    SourceUrl, url="link-1", handler="A", current_status=Statuses.PROCESSED
)


get_cev = partial(
    CollEvent,
    url="link-1",
    handler="A",
    status=Statuses.PROCESSED,
    timestamp=1000,
    output_file="of1",
)


def test_register(dbsession):
    A = "HA"
    B = "HB"

    add_urls(dbsession, A, ["a", "b", "c"])
    add_urls(dbsession, B, ["x", "y", "z"])

    for urls, handler, expected_count in [
        ([], A, 6),
        (["d"], A, 7),
        (["a"], A, 7),
        (["x", "y", "a", "b"], B, 9),
    ]:
        add_urls(dbsession, handler, urls)
        assert dbsession.query(SourceUrl).count() == expected_count


def test_batch(dbsession):
    dbsession.add(get_surl(current_status=Statuses.TODO))
    dbsession.add(get_surl(handler="B", current_status=Statuses.SESSION_BROKEN))
    dbsession.add(get_surl(url="link-3", current_status=Statuses.PROCESSING))
    dbsession.add(get_surl(url="link-4", current_status=Statuses.PARSING_ERROR))
    dbsession.commit()

    batch = get_next_batch(dbsession, 10, to_processing=False)
    assert len(batch) == 2
    assert sorted([Statuses.TODO, Statuses.SESSION_BROKEN]) == sorted(
        [su.current_status for su in batch]
    )

    assert len(get_next_batch(dbsession, 2)) == 2


def test_surl():
    surl = get_surl()
    assert surl.handler in surl.__repr__()


def test_cev(tmp_path):
    cev = get_cev()
    assert cev.status in cev.__repr__()
    cev.dump(tmp_path)
