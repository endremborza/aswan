from contextlib import contextmanager
from multiprocessing import Process, cpu_count
from pathlib import Path
from typing import Dict, List, Optional, Type

from atqo import Scheduler
from parquetranger import TableRepo
from structlog import get_logger

from ..config_class import AswanConfig
from ..connection_session import HandlingTask, get_actor_dict
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
from ..models import CollectionEvent, SourceUrl
from ..monitor_dash.app import run_monitor_app
from ..resources import REnum
from ..security import ProxyData
from ..security.proxy_base import ProxyBase
from ..t2_integrators import T2Integrator
from ..url_handler import UrlHandler  # pragma: no cover
from ..utils import is_handler, is_proxy_base, is_t2_integrator, run_and_log_functions

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

    def __init__(self, config: Optional[AswanConfig] = None, debug=False):

        self.config = config or AswanConfig.default_from_dir(Path.cwd())
        self._current_env = Envs.PROD
        self._handler_dic: Dict[str, "UrlHandler"] = {}
        self._t2int_dic: Dict[str, "T2Integrator"] = {}
        self._t2_tables: Dict[str, TableRepo] = {}

        self._engines, self._obj_stores = self.config.get_db_dicts()

        self._scheduler: Optional[Scheduler] = None
        self._monitor_app_process: Optional[Process] = None

        self._proxy_dic = {UrlHandler.proxy_kind: ProxyData()}
        self._ran_once = False
        self.debug = debug

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

        self._ran_once = False
        self._prepare_run(with_monitor_process)
        self._scheduler.process(
            batch_producer=self._get_next_batch,
            result_processor=self._proc_results,
            min_queue_size=self.env_config.min_queue_size,
        )
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
            target=run_monitor_app,
            kwargs={"port_no": port_no, "conf": self.config},
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

    def add_proxies(self, proxies: Type[ProxyBase]):
        for p in proxies:
            pd = ProxyData(p)
            self._proxy_dic[pd.name] = pd
        logger.info(
            f"added proxies {self._proxy_dic}",
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

    def push(self, clean_ostore=False):
        """push prod to remote"""
        push(self.config.prod, self.config.remote_root, clean_ostore)

    def pull(self, pull_ostore=False):
        """pull from remote to prod"""
        pull(self.config.prod, self.config.remote_root, pull_ostore)

    def integrate_to_t2(self, redo=False):
        # TODO
        # can be problem with table extension
        # if one elem is integrated multiple times
        with self._get_session() as session:
            coll_evs = get_non_integrated(session, redo)
            pcevs = [ParsedCollectionEvent(cev, self) for cev in coll_evs]
            for integrator in self._t2int_dic.values():
                logger.info(f"running integrator {type(integrator).__name__}")
                try:
                    integrator.parse_pcevlist(pcevs)
                except Exception as e:
                    logger.warning(f"integrator raised error: {type(e).__name__}: {e}")
                    raise e
                    return
            for cev in coll_evs:
                cev.integrated_to_t2 = True
            session.commit()

    @property
    def env_config(self):
        return self.config.env_dict()[self._current_env]

    @property
    def object_store(self):
        return self._obj_stores[self._current_env]

    @property
    def resource_limits(self):
        proxy_limits = {
            pd.res_id: pd.limit for pd in self._proxy_dic.values() if pd.limit
        }
        return {
            REnum.mCPU: int(cpu_count() * 1000),
            REnum.DISPLAY: 4,
            **proxy_limits,
        }

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
        cleanup_functions = [self._scheduler.join, self.integrate_to_t2]
        if with_monitor_process:
            cleanup_functions.append(self.stop_monitor_process)
        run_and_log_functions(cleanup_functions, function_batch="run_cleanup")

    def _get_next_batch(self):
        if self._ran_once and (not self.env_config.keep_running):
            return []

        with self._get_session() as session:
            n_to_target = self.env_config.batch_size - self._scheduler.queued_task_count
            next_surl_batch = get_next_surl_batch(
                max(n_to_target, 0),
                session,
            )
        self._ran_once = True
        return self._surls_to_tasks(next_surl_batch)

    def _proc_results(self, processed_results):
        with self._get_session() as session:
            process_result_queue(processed_results, session)

    def _surls_to_tasks(self, surl_batch: List[SourceUrl]):
        task_batch = []
        with self._get_session() as session:
            for next_surl in surl_batch:
                handler = self._handler_dic[next_surl.handler]
                url = next_surl.url
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
                    ).get_scheduler_task(self._proxy_dic)
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

        actor_dict = get_actor_dict(self._proxy_dic.values())

        self._scheduler = Scheduler(
            actor_dict=actor_dict,
            resource_limits=self.resource_limits,
            distributed_system=self.env_config.distributed_api,
            verbose=self.debug,
        )

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
        return self._ostore.read_json(self._output_file) if self._output_file else None

    def __repr__(self):
        return f"{self.status}: {self.handler_name} - {self.url}"
