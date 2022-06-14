from multiprocessing import Process, cpu_count
from typing import Dict, List, Optional, Type

from atqo import Scheduler
from parquetranger import TableRepo
from structlog import get_logger

from .config_class import AswanConfig
from .connection_session import HandlingTask, get_actor_dict
from .metadata_handling import MetaHandler
from .models import CollectionEvent, SourceUrl
from .monitor_app import run_monitor_app
from .resources import REnum
from .security import ProxyData
from .security.proxy_base import ProxyBase
from .t2_integrators import T2Integrator
from .url_handler import UrlHandlerBase
from .utils import is_handler, is_proxy_base, is_t2_integrator, run_and_log_functions

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

    def __init__(self, name: str, debug=False):

        self.config = AswanConfig(name)
        self.env_config = self.config.prod
        self._handler_dic: Dict[str, "UrlHandlerBase"] = {}
        self._t2int_dic: Dict[str, "T2Integrator"] = {}
        self._t2_tables: Dict[str, TableRepo] = {}

        self._meta = MetaHandler(self.config)
        self._scheduler: Optional[Scheduler] = None
        self._monitor_app_process: Optional[Process] = None

        self._proxy_dic = {UrlHandlerBase.proxy_kind: ProxyData()}
        self._ran_once = False
        self.debug = debug

        self.add_urls_to_handler = self._meta.add_urls
        self.reset_surls = self._meta.reset_surls
        self.push = self.config.push
        self.pull = self.config.pull

    def set_env(self, env: str):
        """set env to prod/exp/test

        it is set to prod by default
        """
        self.env_config = self.config.env_dict[env]
        self._meta.set_env(env)
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
            result_processor=self._meta.process_results,
            min_queue_size=self.env_config.min_queue_size,
        )
        self._finalize_run(with_monitor_process)

    def register_handler(self, handler: Type["UrlHandlerBase"]):
        self._handler_dic[handler.__name__] = handler()
        return handler

    def register_t2_integrator(self, integrator: Type["T2Integrator"]):
        self._t2int_dic[integrator.__name__] = integrator()
        return integrator

    def register_t2_table(self, tabrepo: TableRepo):
        self._t2_tables[tabrepo.name] = tabrepo
        return tabrepo

    def register_proxy(self, proxy: Type[ProxyBase]):
        pdata = ProxyData(proxy)
        self._proxy_dic[pdata.name] = pdata
        return proxy

    def register_module(self, mod):
        for e in mod.__dict__.values():
            if is_handler(e):
                self.register_handler(e)
            elif is_t2_integrator(e):
                self.register_t2_integrator(e)
            elif is_proxy_base(e):
                self.register_proxy(e)
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
        handler: Type["UrlHandlerBase"],
        only_successful: bool = True,
        only_latest: bool = True,
        limit=None,
    ):
        for cev in self._meta.get_handler_events(
            handler, only_successful, only_latest, limit
        ):
            yield ParsedCollectionEvent(cev, self)

    def integrate_to_t2(self, redo=False):
        # TODO - make it way better
        # can't do if not pushed/pulled enough
        # can be problem with table extension
        # if one elem is integrated multiple times
        for integrator_name, integrator in self._t2int_dic.items():
            logger.info(f"running integrator {type(integrator).__name__}")
            with self._meta.get_non_integrated(integrator_name, redo) as cevs:
                pcevs = [ParsedCollectionEvent(cev, self) for cev in cevs]
                try:
                    integrator.parse_pcevlist(pcevs)
                except Exception as e:
                    logger.warning(f"integrator raised error: {type(e).__name__}: {e}")
                    raise e

    def get_prod_table(self, name, group_cols=None):
        return self.register_t2_table(self.config.get_prod_table(name, group_cols))

    def purge(self, remote=False):
        self.config.purge(remote)

    @property
    def object_store(self):
        return self.env_config.object_store

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
        if self._current_env_name in [self.config.test_name, self.config.exp_name]:
            prep_functions.append(self._purge_env)

        if self._current_env_name in [self.config.prod_name, self.config.exp_name]:
            prep_functions += [
                self.reset_surls,
                self._meta.expire_surls,
                self._register_starter_urls,
            ]
        else:
            prep_functions.append(self._restore_test_state)

        if self._current_env_name == self.config.exp_name:
            prep_functions.append(self._move_batch_from_prod_to_exp)

        prep_functions.append(self._create_scheduler)

        if with_monitor_process:
            prep_functions.append(self.start_monitor_process)
        run_and_log_functions(
            prep_functions, function_batch="run_prep", env=self._current_env_name
        )

    def _finalize_run(self, with_monitor_process: bool):
        cleanup_functions = [self._scheduler.join, self.integrate_to_t2]
        if with_monitor_process:
            cleanup_functions.append(self.stop_monitor_process)
        run_and_log_functions(cleanup_functions, function_batch="run_cleanup")

    def _get_next_batch(self):
        if self._ran_once and (not self.env_config.keep_running):
            return []
        n_to_target = self.env_config.batch_size - self._scheduler.queued_task_count
        self._ran_once = True
        return self._meta.next_batch(
            max(n_to_target, 0), to_processing=True, parser=self._surls_to_tasks
        )

    def _surls_to_tasks(self, surl_batch: List[SourceUrl]):
        return [
            HandlingTask(
                handler=self._handler_dic[next_surl.handler],
                url=next_surl.url,
                object_store=self.object_store,
            ).get_scheduler_task(self._proxy_dic)
            for next_surl in surl_batch
        ]

    def _purge_env(self):
        self.object_store.purge()
        self._meta.purge_db()
        for trepo in self._t2_tables.values():
            trepo.purge()

    def _restore_test_state(self):
        self.set_env(self.config.test_name)  # just to be sure
        return self._register_initial_urls("test")

    def _register_starter_urls(self) -> int:
        return self._register_initial_urls("starter")

    def _move_batch_from_prod_to_exp(self):
        n = self.env_config.batch_size
        self.set_env(self.config.prod_name)
        surls = self._meta.next_batch(n, expunge=True)
        self.set_env(self.config.exp_name)
        self._meta.save(surls)

    def _register_initial_urls(self, url_kind) -> int:
        n = 0
        for handler_inst in self._handler_dic.values():
            handler_cls = type(handler_inst)
            urls = getattr(handler_cls, f"{url_kind}_urls")
            self._meta.add_urls(handler_cls, urls)
            n += len(urls)
        return n

    def _create_scheduler(self):

        actor_dict = get_actor_dict(self._proxy_dic.values())

        self._scheduler = Scheduler(
            actor_dict=actor_dict,
            resource_limits=self.resource_limits,
            distributed_system=self.env_config.distributed_api,
            verbose=self.debug,
        )

    @property
    def _current_env_name(self):
        return self.env_config.name


class ParsedCollectionEvent:
    def __init__(self, cev: "CollectionEvent", project: Project):
        self.url = cev.url
        self.handler_name = cev.handler
        self._output_file = cev.output_file
        self._ostore = project.object_store
        self._time = cev.timestamp
        self.status = cev.status

    @property
    def content(self):
        # TODO it shouldn't be all json
        return self._ostore.read_json(self._output_file) if self._output_file else None

    def __repr__(self):
        return f"{self.status}: {self.handler_name} - {self.url} ({self._time})"
