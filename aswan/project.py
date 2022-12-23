from functools import partial
from multiprocessing import Process, cpu_count
from typing import Dict, Iterable, List, Optional, Type

from atqo import DEFAULT_DIST_API_KEY, DEFAULT_MULTI_API, Scheduler
from structlog import get_logger

from . import url_handler as urh
from .connection_session import HandlingTask, get_actor_items
from .constants import Statuses
from .depot import AswanDepot, Status
from .models import RegEvent, SourceUrl
from .resources import REnum
from .utils import is_subclass, run_and_log_functions

logger = get_logger()


class Project:
    def __init__(
        self,
        name: str,
        local_root: Optional[str] = None,
        distributed_api=DEFAULT_MULTI_API,
        max_displays: int = 4,
        max_cpu_use: float = float(cpu_count()),
        batch_multiplier=16,
        debug=False,
    ):

        self.depot = AswanDepot(name, local_root)
        self.distributed_api = distributed_api
        self.debug = debug
        self.max_displays = max_displays
        self.max_cpu_use = max_cpu_use
        self.batch_size = max(int(self.max_cpu_use) * batch_multiplier, 40)
        self.min_queue_size = self.batch_size // 2

        self._handler_dic: Dict[str, urh.ANY_HANDLER_T] = {}
        self._scheduler: Optional[Scheduler] = None
        self._monitor_app_process: Optional[Process] = None

        self._ran_once = False
        self._keep_running = True
        self._is_test = False

        self.register_module(urh)

    def run(
        self,
        urls_to_register: Optional[Dict[Type, Iterable[str]]] = None,
        urls_to_overwrite: Optional[Dict[Type, Iterable[str]]] = None,
        test_run=False,
        keep_running=True,
        force_sync=False,
    ):
        """run project

        test runs on a basic local thread
        """

        self._ran_once = False
        self._is_test = test_run

        _prep = [
            self.depot.setup,
            partial(self._initiate_status, urls_to_register, urls_to_overwrite),
        ]
        self._run(force_sync, keep_running, _prep)

    def commit_current_run(self):
        if self._is_test:
            raise PermissionError("last run was a test, do not commit it")
        if self.depot.current.any_in_progress():
            raise ValueError("can't commit, work in progress")
        self.depot.save_current()
        self.depot.current.purge()

    def continue_run(
        self,
        inprogress=True,
        parsing_error=False,
        conn_error=False,
        sess_broken=False,
        force_sync=False,
        keep_running=True,
    ):
        bool_map = {
            Statuses.PROCESSING: inprogress,
            Statuses.PARSING_ERROR: parsing_error,
            Statuses.CONNECTION_ERROR: conn_error,
            Statuses.SESSION_BROKEN: sess_broken,
        }
        statuses = [s for s, b in bool_map.items() if b]
        prep = [partial(self.depot.current.reset_surls, statuses)]
        self.depot.current.setup()
        self._run(force_sync, keep_running, prep)

    def register_handler(self, handler: Type[urh.ANY_HANDLER_T]):
        # called for .name to work later and proxy to init
        if handler.__name__ in self._handler_dic.keys():
            return
        self._handler_dic[handler.__name__] = handler()
        return handler

    def register_module(self, mod):
        for e in mod.__dict__.values():
            if is_subclass(e, urh.UrlHandlerBase):
                self.register_handler(e)

    @property
    def resource_limits(self):
        proxy_limits, handler_limits = {}, {}
        for handler in self._handler_dic.values():
            if handler.proxy.max_at_once:
                proxy_limits[handler.proxy.res_id] = handler.proxy.max_at_once
            if handler.max_in_parallel is not None:
                handler_limits[handler.name] = handler.max_in_parallel
        return {
            REnum.mCPU: int(self.max_cpu_use) * 1000,
            REnum.DISPLAY: self.max_displays,
            **handler_limits,
            **proxy_limits,
        }

    def _run(self, force_sync, keep_running, extra_prep=()):
        self._keep_running = keep_running
        _old_da = self.distributed_api
        if force_sync:
            self.distributed_api = DEFAULT_DIST_API_KEY
        run_and_log_functions([*extra_prep, self._create_scheduler], batch="prep")
        for res in self._scheduler.process(
            batch_producer=self._get_next_batch, min_queue_size=self.min_queue_size
        ):
            if isinstance(res, Exception):
                raise res

        run_and_log_functions([self._scheduler.join], batch="cleanup")
        self.distributed_api = _old_da

    def _get_next_batch(self):
        if self._ran_once and not self._keep_running:
            return []
        n_to_target = self.batch_size - self._scheduler.queued_task_count
        self._ran_once = True
        return self.depot.current.next_batch(
            max(n_to_target, 0), parser=self._surls_to_tasks
        )

    def _surls_to_tasks(self, surl_batch: List[SourceUrl]):
        return [
            HandlingTask(
                handler=self._handler_dic[next_surl.handler], url=next_surl.url
            ).get_scheduler_task()
            for next_surl in surl_batch
        ]

    def _initiate_status(
        self,
        urls_to_register: Optional[Dict[Type[urh.ANY_HANDLER_T], Iterable[str]]],
        urls_to_overwrite: Optional[Dict[Type[urh.ANY_HANDLER_T], Iterable[str]]],
    ):
        reg_events = []
        for url_dic, ovw in [(urls_to_register, False), (urls_to_overwrite, True)]:
            for handler, urls in (url_dic or {}).items():
                reg_events.extend(_get_event_bunch(handler, urls, ovw))
                self.register_handler(handler)

        if self._is_test:
            status = Status()
            for handler in self._handler_dic.values():
                reg_events.extend(_get_event_bunch(type(handler), handler.test_urls))
        else:
            status = self.depot.get_complete_status()

        self.depot.set_as_current(status)
        self.depot.current.integrate_events(reg_events)

    def _create_scheduler(self):
        self._scheduler = Scheduler(
            actor_dict=dict(
                get_actor_items(self._handler_dic.values(), depot_path=self.depot.root)
            ),
            resource_limits=self.resource_limits,
            distributed_system=self.distributed_api,
            verbose=self.debug,
        )


def _get_event_bunch(handler: Type[urh.ANY_HANDLER_T], urls, overwrite=False):
    part = partial(RegEvent, handler=handler.__name__, overwrite=overwrite)
    return map(part, map(handler.extend_link, urls))
