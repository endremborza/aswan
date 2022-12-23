from collections import defaultdict
from functools import partial
from typing import Iterable, List, Union

from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.sql import and_

from .constants import Statuses
from .models import CollEvent, RegEvent, SourceUrl


def add_urls(session: Session, handler: str, urls: Iterable[str], overwrite=False):
    url_set = set(urls)
    filter_q = and_(SourceUrl.url.in_(tuple(url_set)), SourceUrl.handler == handler)
    present = session.query(SourceUrl).filter(filter_q)
    _ud = dict(current_status=Statuses.TODO)
    if overwrite:
        present.update(_ud, synchronize_session="fetch")
    present_urls = set([s.url for s in present.all()])
    ps_url = partial(SourceUrl, handler=handler, **_ud)
    surls = [ps_url(url=url) for url in url_set if url not in present_urls]
    session.bulk_save_objects(surls)
    session.commit()


def update_sources(session: Session, handler: str, urls: Iterable[str], status):
    base_q = session.query(SourceUrl).filter(
        SourceUrl.handler == handler, SourceUrl.url.in_(urls)
    )
    if status in [Statuses.PROCESSED, Statuses.CACHE_LOADED]:
        base_q.delete()
    else:
        base_q.update({"current_status": status})


def get_next_batch(
    session: Session, size: int, to_processing=True, parser=list
) -> List[SourceUrl]:
    to_try = [Statuses.TODO, Statuses.SESSION_BROKEN]
    query = (
        session.query(SourceUrl)
        .filter(SourceUrl.current_status.in_(to_try))
        .limit(size)
    )

    surls = query.all()
    if to_processing:
        for surl in surls:
            surl.current_status = Statuses.PROCESSING
        session.commit()
    return parser(surls)


def get_grouped_surls(session: Session):
    return (
        session.query(SourceUrl.current_status, SourceUrl.handler, func.count())
        .group_by(SourceUrl.current_status, SourceUrl.handler)
        .all()
    )


def reset_surls(session: Session, statuses):
    session.query(SourceUrl).filter(
        SourceUrl.current_status.in_(tuple(statuses))
    ).update({"current_status": Statuses.TODO}, synchronize_session="fetch")
    session.commit()


def integrate_events(
    session: Session, events: Iterable[Union[RegEvent, CollEvent]], dump_dir=None
):
    reg_urls = defaultdict(list)
    coll_urls = defaultdict(list)
    for event in events:
        if dump_dir:
            event.dump(dump_dir)
        if isinstance(event, RegEvent):
            suffix = event.overwrite
            dic = reg_urls
        elif isinstance(event, CollEvent):
            suffix = event.status
            dic = coll_urls
        dic[(event.handler, suffix)].append(event.url)

    for url_dic, fun in [
        (coll_urls, update_sources),
        (reg_urls, add_urls),
    ]:
        for (handler, suff), urls in url_dic.items():
            fun(session, handler, urls, suff)
    session.commit()
