import time
from typing import TYPE_CHECKING, Iterable, List, Type

from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import and_, func

from .constants import Statuses
from .db import Session
from .models import CollectionEvent, SourceUrl

if TYPE_CHECKING:
    from .connection_session import UrlHandlerResult  # pragma: nocover
    from .url_handler import UrlHandler  # pragma: nocover


def register_source_urls(
    surls: Iterable["SourceUrl"],
    session: Session,
    overwrite=False,
):
    for surl in surls:
        session.begin_nested()
        session.add(surl)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            if overwrite:
                session.query(SourceUrl).filter(
                    and_(
                        SourceUrl.url == surl.url,
                        SourceUrl.handler == surl.handler,
                    )
                ).update(surl.to_update_dict())
                session.commit()
    session.commit()


def get_next_surl_batch(batch_size: int, session: Session) -> List[SourceUrl]:
    return (
        session.query(SourceUrl)
        .filter(
            SourceUrl.current_status.in_(
                [Statuses.TODO, Statuses.EXPIRED, Statuses.SESSION_BROKEN]
            )
        )
        .limit(batch_size)
        .all()
    )


def get_non_integrated(session: Session) -> List[CollectionEvent]:
    return (
        session.query(CollectionEvent)
        .filter(
            CollectionEvent.status == Statuses.PROCESSED,
            CollectionEvent.integrated_to_t2 == False  # noqa: E712
        )
        .all()
    )


def update_surl_status(
    handler_name: str,
    url: str,
    status: str,
    session: Session,
    expiry_seconds: int = None
):
    update_dic = {"current_status": status}
    if expiry_seconds is not None:
        update_dic["expiry_seconds"] = expiry_seconds
    session.query(SourceUrl).filter(
        SourceUrl.handler == handler_name,
        SourceUrl.url == url,
    ).update(update_dic)
    session.commit()


def reset_surls(session: Session, statuses: list):
    session.query(SourceUrl).filter(
        SourceUrl.current_status.in_(tuple(statuses))
    ).update({"current_status": Statuses.TODO}, synchronize_session='fetch')
    session.commit()


def process_result_queue(
    result_queue: Iterable["UrlHandlerResult"], session: Session
):
    s_urls = []
    for uh_result in result_queue:
        registered_surls = [
            SourceUrl(
                url=rl.url,
                handler=rl.handler_name,
                current_status=Statuses.TODO,
            )
            for rl in uh_result.registered_links
        ]
        update_surl_status(
            uh_result.handler_name,
            uh_result.url,
            uh_result.status,
            session,
            uh_result.expiration_seconds
        )
        session.add(
            CollectionEvent(
                handler=uh_result.handler_name,
                url=uh_result.url,
                status=uh_result.status,
                timestamp=uh_result.timestamp,
                output_file=uh_result.output_file,
            )
        )
        s_urls += registered_surls

    session.commit()
    register_source_urls(s_urls, session)


def get_handler_events(
    handler: Type["UrlHandler"],
    session: Session,
    only_successful: bool = True,
    only_latest: bool = True,
    limit=None,
):
    base_q = session.query(CollectionEvent).filter(
        CollectionEvent.handler == handler.__name__
    )
    if only_latest:
        subq = (
            session.query(
                CollectionEvent.url.label("curl"),
                func.max(CollectionEvent.timestamp).label("maxtimestamp"),
            )
            .filter(CollectionEvent.handler == handler.__name__)
            .group_by(CollectionEvent.url)
            .subquery()
        )

        base_q = base_q.join(
            subq,
            and_(
                CollectionEvent.url == subq.c.curl,
                CollectionEvent.timestamp == subq.c.maxtimestamp,
            ),
        )
    if only_successful:
        base_q = base_q.filter(CollectionEvent.status == Statuses.PROCESSED)
    if limit:
        base_q = base_q.limit(limit)
    return base_q.all()


def expire_surls(session: Session):
    subq = (
        session.query(
            CollectionEvent.url.label("curl"),
            CollectionEvent.handler.label("chandler"),
            func.max(CollectionEvent.timestamp).label("maxtimestamp"),
        )
        .filter(SourceUrl.expiry_seconds > -1)
        .join(
            SourceUrl,
            and_(
                CollectionEvent.status == Statuses.PROCESSED,
                CollectionEvent.handler == SourceUrl.handler,
                CollectionEvent.url == SourceUrl.url,
            ),
        )
        .group_by(CollectionEvent.url)
        .subquery()
    )

    exps = (
        session.query(SourceUrl)
        .join(
            subq,
            and_(
                subq.c.curl == SourceUrl.url,
                subq.c.chandler == SourceUrl.handler,
            ),
        )
        .filter(subq.c.maxtimestamp < (time.time() - SourceUrl.expiry_seconds))
    )

    for exp in exps:
        update_surl_status(
            handler_name=exp.handler,
            url=exp.url,
            status=Statuses.EXPIRED,
            session=session,
        )


def purge_db(session: Session):
    session.query(CollectionEvent).delete()
    session.query(SourceUrl).delete()
    session.commit()
