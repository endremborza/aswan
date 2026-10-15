from functools import partial

import pytest

from aswan.constants import Statuses
from aswan.metadata_handling import (
    add_urls,
    get_next_batch,
    integrate_events,
    update_sources,
)
from aswan.models import CollEvent, RegEvent, SourceUrl

from ..helpers import get_cev

get_surl = partial(
    SourceUrl, url="link-1", handler="A", current_status=Statuses.PROCESSED
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


def test_dedup_keeps_succeeded_urls(dbsession):
    add_urls(dbsession, "A", ["u1"])
    update_sources(dbsession, "A", ["u1"], Statuses.PROCESSED)
    dbsession.commit()

    def _status():
        return dbsession.query(SourceUrl).filter_by(url="u1").one().current_status

    assert _status() == Statuses.PROCESSED
    # re-registering without overwrite must not resurrect a succeeded url
    add_urls(dbsession, "A", ["u1"])
    assert _status() == Statuses.PROCESSED
    # overwrite forces a refetch
    add_urls(dbsession, "A", ["u1"], overwrite=True)
    assert _status() == Statuses.TODO


def test_self_registration_does_not_loop(dbsession):
    add_urls(dbsession, "A", ["u1"])
    integrate_events(
        dbsession,
        [
            CollEvent(
                url="u1",
                handler="A",
                status=Statuses.PROCESSED,
                timestamp=1,
                output_file="",
            ),
            RegEvent(url="u1", handler="A"),
        ],
    )
    row = dbsession.query(SourceUrl).filter_by(url="u1").one()
    assert row.current_status == Statuses.PROCESSED
    assert get_next_batch(dbsession, 10, to_processing=False) == []


def test_latest_collection_wins_regardless_of_order(dbsession):
    add_urls(dbsession, "A", ["u1"])
    newer = get_cev(url="u1", status=Statuses.PROCESSED, timestamp=2)
    older = get_cev(url="u1", status=Statuses.CONNECTION_ERROR, timestamp=1)
    integrate_events(dbsession, [newer, older])
    row = dbsession.query(SourceUrl).filter_by(url="u1").one()
    assert row.current_status == Statuses.PROCESSED


@pytest.mark.parametrize("order", [1, -1])
def test_session_break_loses_a_same_second_tie(dbsession, order):
    add_urls(dbsession, "A", ["u1"])
    broken = get_cev(url="u1", status=Statuses.SESSION_BROKEN, timestamp=5)
    failed = get_cev(url="u1", status=Statuses.CONNECTION_ERROR, timestamp=5)
    integrate_events(dbsession, [broken, failed][::order])
    row = dbsession.query(SourceUrl).filter_by(url="u1").one()
    assert row.current_status == Statuses.CONNECTION_ERROR


def test_surl():
    surl = get_surl()
    assert surl.handler in surl.__repr__()


def test_cev(tmp_path):
    cev = get_cev()
    assert cev.status in cev.__repr__()
    cev.dump(tmp_path)
