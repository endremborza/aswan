import time
from collections import defaultdict
from contextlib import contextmanager
from functools import partial, wraps
from typing import TYPE_CHECKING, Iterable, List, Type

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import and_, func

from .constants import Statuses
from .models import Base, CollectionEvent, IntegrationEvent, SourceUrl

if TYPE_CHECKING:
    from .config_class import AswanConfig  # pragma: no cover
    from .connection_session import UrlHandlerResult  # pragma: no cover
    from .url_handler import UrlHandlerBase  # pragma: no cover


MySession = sessionmaker()


class MetaHandler:
    def __init__(self, conf: "AswanConfig") -> None:
        self._conf = conf
        self.env_conf = conf.prod
        self.engine = self.env_conf.get_engine()

        self.purge_db = self._wrap(purge_db)
        self.add_urls = self._wrap(add_urls_to_handler)
        self.validate_pull = self._wrap(validate_pull)
        self.validate_push = self._wrap(validate_push)
        self.next_batch = self._wrap(get_next_batch)
        self.process_results = self._wrap(process_result_queue)
        self.expire_surls = self._wrap(expire_surls)
        self.get_handler_events = self._wrap(get_handler_events)
        self.reset_surls = self._wrap(reset_surls)
        self.save = self._wrap(_save)

    def set_env(self, env_name):
        self.env_conf = self._conf.env_dict[env_name]
        self.engine = self.env_conf.get_engine()

    @contextmanager
    def get_non_integrated(self, integrator: str, redo=False, only_latest=True):
        with self._get_session() as session:
            filters = [CollectionEvent.status == Statuses.PROCESSED]
            if not redo:
                integ_query = (
                    session.query(IntegrationEvent)
                    .filter_by(integrator=integrator)
                    .with_entities(IntegrationEvent.cev)
                )
                filters.append(CollectionEvent.cid.notin_(integ_query))
            query = session.query(CollectionEvent).filter(*filters)
            if only_latest:
                query = _to_latest(query)
            cevs = query.all()
            yield cevs
            ts = int(time.time())
            piev = partial(IntegrationEvent.create, integrator=integrator, ts=ts)
            recs = [piev(cev=cev.cid) for cev in cevs]
            session.bulk_save_objects(recs)
            session.commit()

    @contextmanager
    def _get_session(self):
        session: Session = MySession(bind=self.engine)
        yield session
        session.close()

    def _wrap(self, fun):
        @wraps(fun)
        def f(*args, **kwargs):
            with self._get_session() as session:
                return fun(session, *args, **kwargs)

        return f


def purge_db(session: Session):
    session.query(CollectionEvent).delete()
    session.query(SourceUrl).delete()
    session.commit()


def validate_push(session: Session, target_engine):
    Base.metadata.create_all(target_engine)
    target_session: Session = MySession(bind=target_engine)
    _validate(session, target_session)
    target_session.close()


def validate_pull(session, source_engine):
    Base.metadata.create_all(source_engine)
    source_session: Session = MySession(bind=source_engine)
    _validate(source_session, session)
    source_session.close()


def _validate(source_session: Session, target_session: Session, batch=10_000):
    pushed_hashes = target_session.query(IntegrationEvent).with_entities(
        IntegrationEvent.md5hash
    )
    start = 0
    while True:
        hash_batch = [t[0] for t in pushed_hashes.slice(start, start + batch)]
        start += batch
        n = len(hash_batch)
        if n == 0:
            break
        source_batch = source_session.query(IntegrationEvent).filter(
            IntegrationEvent.md5hash.in_(hash_batch)
        )
        assert source_batch.count() == n, "Can't migrate, missing T2 integrations"


def add_urls_to_handler(
    session: Session,
    handler: Type["UrlHandlerBase"],
    raw_urls: Iterable[str],
    overwrite=False,
):
    handler_name = handler.__name__
    urls = set(map(handler.extend_link, raw_urls))
    filt_q = and_(SourceUrl.url.in_(tuple(urls)), SourceUrl.handler == handler_name)
    present = session.query(SourceUrl).filter(filt_q)
    _ud = dict(current_status=Statuses.TODO, expiry_seconds=handler.default_expiration)
    if overwrite:
        present.update(_ud, synchronize_session="fetch")
    present_urls = set([s.url for s in present.all()])
    ps_url = partial(SourceUrl, handler=handler_name, **_ud)
    surls = [ps_url(url=url) for url in urls if url not in present_urls]
    session.bulk_save_objects(surls)
    session.commit()


def get_next_batch(
    session: Session, size: int, expunge=False, to_processing=False, parser=list
) -> List[SourceUrl]:
    to_try = [Statuses.TODO, Statuses.EXPIRED, Statuses.SESSION_BROKEN]
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
    if expunge:
        [*map(session.expunge, surls)]
    return parser(surls)


def update_surl_status(
    handler_name: str,
    url: str,
    status: str,
    session: Session,
    expiry_seconds: int = None,
):
    update_dic = {"current_status": status}
    if expiry_seconds is not None:
        update_dic["expiry_seconds"] = expiry_seconds
    session.query(SourceUrl).filter(
        SourceUrl.handler == handler_name,
        SourceUrl.url == url,
    ).update(update_dic)
    session.commit()


def process_result_queue(session: Session, result_queue: Iterable["UrlHandlerResult"]):
    regs = defaultdict(list)
    for uh_result in result_queue:
        for rl in uh_result.registered_links:
            regs[rl.handler_cls].append(rl.url)
        update_surl_status(
            uh_result.handler_name,
            uh_result.url,
            uh_result.status,
            session,
            uh_result.expiration_seconds,
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
    session.commit()
    for handler, urls in regs.items():
        add_urls_to_handler(session, handler, urls)
        # TODO: overwrite? expiration?


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


def get_handler_events(
    session: Session,
    handler: Type["UrlHandlerBase"],
    only_successful: bool = True,
    only_latest: bool = True,
    limit=None,
):
    base_q = session.query(CollectionEvent).filter(
        CollectionEvent.handler == handler.__name__
    )
    if only_latest:
        base_q = _to_latest(base_q)
    if only_successful:
        base_q = base_q.filter(CollectionEvent.status == Statuses.PROCESSED)
    if limit:
        base_q = base_q.limit(limit)
    return base_q.all()


def reset_surls(
    session: Session,
    inprogress=True,
    parsing_error=False,
    conn_error=False,
    sess_broken=False,
):
    bool_map = {
        Statuses.PROCESSING: inprogress,
        Statuses.PARSING_ERROR: parsing_error,
        Statuses.CONNECTION_ERROR: conn_error,
        Statuses.SESSION_BROKEN: sess_broken,
    }
    statuses = [s for s, b in bool_map.items() if b]
    session.query(SourceUrl).filter(
        SourceUrl.current_status.in_(tuple(statuses))
    ).update({"current_status": Statuses.TODO}, synchronize_session="fetch")
    session.commit()


def _to_latest(base_query):

    subq = (
        base_query.group_by(CollectionEvent.url)
        .add_columns(
            CollectionEvent.url.label("curl"),
            func.max(CollectionEvent.timestamp).label("maxtimestamp"),
        )
        .subquery()
    )

    return base_query.join(
        subq,
        and_(
            CollectionEvent.url == subq.c.curl,
            CollectionEvent.timestamp == subq.c.maxtimestamp,
        ),
    )


def _save(session: Session, objs):
    session.bulk_save_objects(objs)
