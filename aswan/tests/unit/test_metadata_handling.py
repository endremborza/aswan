import time

from aswan import RequestHandler
from aswan.constants import Statuses
from aswan.metadata_handling import (
    add_urls_to_handler,
    expire_surls,
    get_next_batch,
    purge_db,
)
from aswan.models import CollectionEvent, SourceUrl


def _getsurl(
    url="link-1",
    handler="A",
    current_status=Statuses.PROCESSED,
    expiry_seconds=-1,
):
    return SourceUrl(
        url=url,
        handler=handler,
        current_status=current_status,
        expiry_seconds=expiry_seconds,
    )


def _getcev(
    url="link-1",
    handler="A",
    status=Statuses.PROCESSED,
    timestamp=1000,
    output_file="of1",
):
    return CollectionEvent(
        url=url,
        handler=handler,
        timestamp=timestamp,
        status=status,
        output_file=output_file,
    )


def test_register(dbsession):
    class A(RequestHandler):
        pass

    class B(RequestHandler):
        pass

    add_urls_to_handler(dbsession, A, ["a", "b", "c"])
    add_urls_to_handler(dbsession, B, ["x", "y", "z"])

    for urls, handler, expected_count in [
        ([], A, 6),
        (["d"], A, 7),
        (["a"], A, 7),
        (["x", "y", "a", "b"], B, 9),
    ]:
        add_urls_to_handler(dbsession, handler, urls)
        assert dbsession.query(SourceUrl).count() == expected_count


def test_batch(dbsession):
    dbsession.add(_getsurl(current_status=Statuses.TODO))
    dbsession.add(_getsurl("link-2", current_status=Statuses.EXPIRED))
    dbsession.add(_getsurl(handler="B", current_status=Statuses.SESSION_BROKEN))
    dbsession.add(_getsurl("link-3", current_status=Statuses.PROCESSING))
    dbsession.add(_getsurl("link-4", current_status=Statuses.PARSING_ERROR))
    dbsession.commit()

    batch = get_next_batch(dbsession, 10)
    assert len(batch) == 3
    assert sorted([Statuses.TODO, Statuses.EXPIRED, Statuses.SESSION_BROKEN]) == sorted(
        [su.current_status for su in batch]
    )

    assert len(get_next_batch(dbsession, 2)) == 2


def test_expiry(dbsession):
    for surl in [
        _getsurl(),
        _getsurl("link-2", expiry_seconds=100),
        _getsurl(handler="B", expiry_seconds=100),
        _getsurl("link-3", expiry_seconds=10),
    ]:
        dbsession.add(surl)
    dbsession.commit()

    for cev, expected_expired in [
        (_getcev(timestamp=time.time()), 0),
        (_getcev("link-2", timestamp=time.time() - 200), 1),
        (_getcev(handler="B", timestamp=time.time() + 200), 1),
        (_getcev(handler="B", timestamp=time.time() - 200), 1),
        (
            _getcev(
                "link-3",
                timestamp=time.time() + 20,
                status=Statuses.PARSING_ERROR,
            ),
            1,
        ),
        (_getcev("link-3", timestamp=time.time() - 20), 2),
    ]:
        dbsession.add(cev)
        dbsession.commit()
        expire_surls(dbsession)
        assert (
            dbsession.query(SourceUrl)
            .filter(SourceUrl.current_status == Statuses.EXPIRED)
            .count()
            == expected_expired
        )


def test_purge(dbsession):
    dbsession.add(_getcev())
    dbsession.add(_getsurl())
    dbsession.commit()
    assert dbsession.query(SourceUrl).count() == 1
    assert dbsession.query(CollectionEvent).count() == 1
    purge_db(dbsession)
    assert dbsession.query(SourceUrl).count() == 0
    assert dbsession.query(CollectionEvent).count() == 0
