import os
import sys
from contextlib import contextmanager
from multiprocessing import Process
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Type

import sqlalchemy as db
from parquetranger import TableRepo
from structlog import get_logger

from ..config_class import AswanConfig
from ..connection_session import ConnectionSession, HandlingTask
from ..constants import Envs, Statuses
from ..db import Session
from ..metadata_handling import (
    expire_surls,
    get_handler_events,
    get_next_surl_batch,
    get_non_integrated,
    process_result_queue,
    purge_db,
    register_source_urls,
    reset_surls,
    update_surl_status,
)
from ..migrate import pull, push
from ..models import Base, CollectionEvent, SourceUrl
from ..monitor_dash.app import get_monitor_app
from ..object_store import get_object_store
from ..scheduler import Scheduler
from ..t2_integrators import T2Integrator
from ..utils import (
    is_handler,
    is_proxy_base,
    is_t2_integrator,
    run_and_log_functions,
)

if TYPE_CHECKING:
    from ..url_handler import UrlHandler  # pragma: no cover

logger = get_logger()


class Project:
    """
    set env with `set_env` function

    afterwards everything identical, except setup_env
    function that runs at the beginning of every run
    and test or exp only run one round
    - test: resets to base test state
            adds test_urls
    - exp: purges everything from exp, copies the first batch from
           prod surls to exp
    - prod: finds and sets expired surls
            adds starter_urls
    """

    def __init__(self, config: Optional[AswanConfig] = None):

        self.config = config or AswanConfig.default_from_dir(Path.cwd())
        self._current_env = Envs.PROD
        self._proxy_dic = {}
        self._handler_dic: Dict[str, "UrlHandler"] = {}
        self._t2int_dic: Dict[str, "T2Integrator"] = {}
        self._t2_tables: Dict[str, TableRepo] = {}

        self._engines = {}
        self._obj_stores = {}
        for name, envconf in self.config.env_items():
            _engine = db.create_engine(envconf.db)
            Base.metadata.create_all(_engine)
            self._engines[name] = _engine
            self._obj_stores[name] = get_object_store(envconf.object_store)

        self._scheduler: Optional[Scheduler] = None
        self._monitor_app_process: Optional[Process] = None

    def set_env(self, env: str):
        """set env to prod/exp/test

        it is set to prod by default
        """
        assert env in Envs.all()
        self._current_env = env
        for trepo in self._t2_tables.values():
            trepo.set_env(env)

    def run(self, with_monitor_process: bool = False):
        """run project

        on one of 3 environments:
        - **test**: totally separate from prod, just to checkout what works
          only runs one round
        - **exp**: reads from prod db and object store, but writes to exp
        - **prod**: reads and writes in prod

        test runs on a basic local thread
        """

        self._prepare_run(with_monitor_process)
        ran_once = False

        while True:
            is_done = self._scheduler.is_idle
            next_surl_batch = self._prep_next_batch()
            if ran_once and not self.env_config.keep_running:
                break
            no_more_surls = len(next_surl_batch) == 0
            if is_done and no_more_surls:
                break
            if no_more_surls:
                self._scheduler.wait_until_n_tasks_remain(0)
                continue
            task_batch = self._surls_to_tasks(next_surl_batch)
            self._scheduler.refill_task_queue(task_batch)
            try:
                self._scheduler.wait_until_n_tasks_remain(
                    self.env_config.min_queue_size
                )
            except KeyboardInterrupt:  # pragma: nocover
                logger.warning("Interrupted waiting for ...")
                break
            ran_once = True
        self._finalize_run(with_monitor_process)

    def register_handler(self, handler: Type["UrlHandler"]):
        self._handler_dic[handler.__name__] = handler()
        return handler

    def register_t2_integrator(self, integrator: Type["T2Integrator"]):
        self._t2int_dic[integrator.__name__] = integrator()
        return integrator

    def register_t2_table(self, tabrepo: TableRepo):
        self._t2_tables[tabrepo.name] = tabrepo

    def register_module(self, mod):
        for e in mod.__dict__.values():
            if is_handler(e):
                self.register_handler(e)
            elif is_t2_integrator(e):
                self.register_t2_integrator(e)
            elif is_proxy_base(e):
                self.add_proxies([e])
            elif isinstance(e, TableRepo):
                self.register_t2_table(e)

    def start_monitor_process(self, port_no=6969):
        self._monitor_app_process = Process(
            target=self._run_monitor_app,
            kwargs={"port_no": port_no},
        )
        self._monitor_app_process.start()
        logger.info(f" monitor app at: http://localhost:{port_no}")

    def stop_monitor_process(self):
        self._monitor_app_process.terminate()
        self._monitor_app_process.join()

    def handler_events(
        self,
        handler: Type["UrlHandler"],
        only_successful: bool = True,
        only_latest: bool = True,
        limit=None,
    ):
        for cev in self._get_handler_events(
            handler, only_successful, only_latest, limit
        ):
            yield ParsedCollectionEvent(cev, self)

    def add_proxies(self, proxies):
        for p in proxies:
            try:
                k = p.name
            except AttributeError:
                k = p.__name__
            self._proxy_dic[k] = p
        logger.info(
            "added proxies",
            **{k: v.__name__ for k, v in self._proxy_dic.items()},
        )

    def add_urls_to_handler(self, handler_kls, urls, overwrite=False):
        with self._get_session() as session:
            register_source_urls(
                [
                    SourceUrl(
                        handler=handler_kls.__name__,
                        url=url,
                        current_status=Statuses.TODO,
                        expiry_seconds=handler_kls.default_expiration,
                    )
                    for url in urls
                ],
                session,
                overwrite=overwrite,
            )

    def reset_surls(
        self,
        inprogress=True,
        parsing_error=False,
        conn_error=False,
        sess_broken=False,
    ):
        statuses = []
        for chk, status in zip(
            [inprogress, parsing_error, conn_error, sess_broken],
            [
                Statuses.PROCESSING,
                Statuses.PARSING_ERROR,
                Statuses.CONNECTION_ERROR,
                Statuses.SESSION_BROKEN,
            ],
        ):
            if chk:
                statuses.append(status)
        with self._get_session() as session:
            reset_surls(session, statuses)

    def push(self):
        """push prod to remote"""
        push(self.config.prod, self.config.remote_root)

    def pull(self):
        """pull from remote to prod"""
        pull(self.config.prod, self.config.remote_root)

    @property
    def env_config(self):
        return self.config.env_dict()[self._current_env]

    @property
    def object_store(self):
        return self._obj_stores[self._current_env]

    def _prepare_run(self, with_monitor_process: bool):
        prep_functions = []
        if self._current_env in [Envs.TEST, Envs.EXP]:
            prep_functions.append(self._purge_env)

        if self._current_env in [Envs.PROD, Envs.EXP]:
            prep_functions += [
                self.reset_surls,
                self._expire_surls,
                self._register_starter_urls,
            ]
        else:
            prep_functions.append(self._restore_test_state)

        if self._current_env == Envs.EXP:
            prep_functions.append(self._move_batch_from_test_to_exp)

        prep_functions.append(self._create_scheduler)

        if with_monitor_process:
            prep_functions.append(self.start_monitor_process)
        run_and_log_functions(
            prep_functions, function_batch="run_prep", env=self._current_env
        )

    def _finalize_run(self, with_monitor_process: bool):
        cleanup_functions = [self._scheduler.join, self._integrate_to_t2]
        if with_monitor_process:
            cleanup_functions.append(self.stop_monitor_process)
        run_and_log_functions(cleanup_functions, function_batch="run_cleanup")

    def _run_monitor_app(self, port_no=6969):
        sys.stdout = open(os.devnull, "w")
        sys.stderr = open(os.devnull, "w")
        get_monitor_app(self._engines, self._obj_stores).run_server(
            port=port_no, debug=False
        )

    def _prep_next_batch(self):
        with self._get_session() as session:
            process_result_queue(
                self._scheduler.get_processed_results(), session
            )

            next_surl_batch = get_next_surl_batch(
                max(
                    self.env_config.batch_size
                    - self._scheduler.queued_task_count,
                    0,
                ),
                session,
            )
        return next_surl_batch

    def _surls_to_tasks(self, surl_batch):
        task_batch = []
        for next_surl in surl_batch:
            handler = self._handler_dic[next_surl.handler]
            url = next_surl.url

            with self._get_session() as session:
                update_surl_status(
                    next_surl.handler,
                    url,
                    Statuses.PROCESSING,
                    session,
                )

                task_batch.append(
                    HandlingTask(
                        handler=handler,
                        url=url,
                        object_store=self.object_store,
                        proxy_dic=self._proxy_dic,
                    ).get_scheduler_task()
                )
        return task_batch

    def _purge_env(self):
        self.object_store.purge()
        with self._get_session() as session:
            purge_db(session)
        for trepo in self._t2_tables.values():
            trepo.purge()

    def _expire_surls(self):
        with self._get_session() as session:
            expire_surls(session)

    def _restore_test_state(self):
        self.set_env(Envs.TEST)  # just to be sure
        return self._register_initial_urls("test")

    def _register_starter_urls(self) -> int:
        return self._register_initial_urls("starter")

    def _move_batch_from_test_to_exp(self):
        n = self.env_config.batch_size
        self.set_env(Envs.PROD)
        with self._get_session() as session:
            surls = get_next_surl_batch(n, session)
            [*map(session.expunge, surls)]
        self.set_env(Envs.EXP)
        with self._get_session() as session:
            register_source_urls(surls, session)

    def _register_initial_urls(self, url_kind) -> int:
        n = 0
        with self._get_session() as session:
            for handler in self._handler_dic.values():
                surls = [
                    SourceUrl(
                        handler=handler.name,
                        url=handler.extend_link(url),
                        current_status=Statuses.TODO,
                        expiry_seconds=handler.default_expiration,
                    )
                    for url in getattr(handler, f"{url_kind}_urls")
                ]
                register_source_urls(
                    surls,
                    session,
                )
                n += len(surls)
        return n

    def _create_scheduler(self):

        ConnectionSession.proxy_dic = self._proxy_dic

        self._scheduler = Scheduler(
            actor_frame=ConnectionSession,
            resource_limits=[],
            distributed_system=self.env_config.distributed_api,
        )  # TODO add resource limits properly

    def _get_handler_events(
        self,
        handler: Type["UrlHandler"],
        only_successful: bool = True,
        only_latest: bool = True,
        limit=None,
    ) -> List["CollectionEvent"]:
        with self._get_session() as session:
            return get_handler_events(
                handler, session, only_successful, only_latest, limit
            )

    def _integrate_to_t2(self):
        # TODO
        # can be problem with table extension
        # if one elem is integrated multiple times
        with self._get_session() as session:
            coll_evs = get_non_integrated(session)
            pcevs = [ParsedCollectionEvent(cev, self) for cev in coll_evs]
            for integrator in self._t2int_dic.values():
                logger.info(f"running integrator {type(integrator).__name__}")
                try:
                    integrator.parse_pcevlist(pcevs)
                except Exception as e:
                    logger.warning(
                        f"integrator raised error: {type(e).__name__}: {e}"
                    )
                    return
            for cev in coll_evs:
                cev.integrated_to_t2 = True
            session.commit()

    @contextmanager
    def _get_session(self) -> "Session":
        session = Session(bind=self._engines[self._current_env])
        yield session
        session.close()


class ParsedCollectionEvent:
    def __init__(self, cev: "CollectionEvent", project: Project):
        self.url = cev.url
        self.handler_name = cev.handler
        self._output_file = cev.output_file
        self._ostore = project.object_store
        self.status = cev.status

    @property
    def content(self):
        return (
            self._ostore.read_json(self._output_file)
            if self._output_file
            else None
        )

    def __repr__(self):
        return f"{self.status}: {self.handler_name} - {self.url}"
