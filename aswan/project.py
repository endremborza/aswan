from functools import partial
from multiprocessing import cpu_count

from atqo import MultiProcAPI, Scheduler, SchedulerTask

from . import url_handler as urh
from .connection_session import HandlingTask, get_session_slot
from .constants import Statuses
from .depot import AswanDepot, Status
from .models import RegEvent, SourceUrl
from .resources import DISPLAY, MCPU
from .utils import is_subclass, run_and_log_functions


class Project:
    def __init__(
        self,
        name: str,
        local_root: str | None = None,
        max_displays: int = 4,
        max_cpu_use: float = float(cpu_count()),
        batch_multiplier=16,
        debug=False,
        stall_timeout: float | None = None,
        task_timeout: float | None = None,
    ):
        self.depot = AswanDepot(name, local_root)
        self.debug = debug
        # Hang guards (default off): stall_timeout aborts a run if the whole
        # scheduler makes no progress for that long; task_timeout fails an
        # individual task that runs longer. Set them to bound worst-case runs.
        self.stall_timeout = stall_timeout
        self.task_timeout = task_timeout
        self.max_displays = max_displays
        self.max_cpu_use = max_cpu_use
        self.batch_size = max(int(self.max_cpu_use) * batch_multiplier, 40)
        self.min_queue_size = self.batch_size // 2

        self._handler_dic: dict[str, urh.ANY_HANDLER_T] = {}
        self._scheduler: Scheduler | None = None

        self._ran_once = False
        self._keep_running = True

        self.register_module(urh)

    def run(
        self,
        urls_to_register=None,
        urls_to_overwrite=None,
        test_run=False,
        keep_running=True,
    ):
        """Run the project.

        With ``test_run=True`` each handler's ``test_urls`` are collected into a
        throwaway run that can't be committed and that the next ``run`` discards.
        """

        self._ran_once = False
        _prep = [
            self.depot.setup,
            partial(
                self._initiate_status, urls_to_register, urls_to_overwrite, test_run
            ),
        ]
        self._run(keep_running, _prep)

    def commit_current_run(self):
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
        urls_to_register=None,
        urls_to_overwrite=None,
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
        reg_events = self._get_reg_events(urls_to_register, urls_to_overwrite)
        self.depot.current.integrate_events(reg_events)
        self._run(keep_running, prep)

    def register_handler(self, handler: type[urh.ANY_HANDLER_T]):
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
    def resources(self) -> dict[str, int]:
        out: dict[str, int] = {
            MCPU: int(self.max_cpu_use) * 1000,
            DISPLAY: self.max_displays,
        }
        for handler in self._handler_dic.values():
            if handler.proxy.max_at_once:
                out[handler.proxy.res_id] = handler.proxy.max_at_once
            if handler.max_in_parallel is not None:
                out[handler.name] = handler.max_in_parallel
        return out

    def _run(self, keep_running, extra_prep=()):
        self._keep_running = keep_running
        run_and_log_functions([*extra_prep, self._create_scheduler], batch="prep")
        for res in self._scheduler.process(
            batch_producer=self._get_next_batch, min_queue_size=self.min_queue_size
        ):
            if isinstance(res, Exception):
                raise res
            if res:
                self.depot.current.apply_events(res)

        run_and_log_functions([self._scheduler.join], batch="cleanup")

    def _get_next_batch(self):
        if self._ran_once and not self._keep_running:
            return []
        n_to_target = self.batch_size - self._scheduler.queued_task_count
        self._ran_once = True
        return self.depot.current.next_batch(
            max(n_to_target, 0), parser=self._surls_to_tasks
        )

    def _surls_to_tasks(self, surl_batch: list[SourceUrl]):
        return [
            SchedulerTask(
                argument=HandlingTask(
                    handler=self._handler_dic[next_surl.handler], url=next_surl.url
                ),
                actor=next_surl.handler,
            )
            for next_surl in surl_batch
        ]

    def _initiate_status(self, urls_to_register, urls_to_overwrite, test_run):
        reg_events = self._get_reg_events(urls_to_register, urls_to_overwrite)
        if test_run:
            status = Status()
            for handler in self._handler_dic.values():
                reg_events.extend(_get_event_bunch(type(handler), handler.test_urls))
        else:
            status = self.depot.get_complete_status()

        self.depot.set_as_current(status, is_test=test_run)
        self.depot.current.integrate_events(reg_events)

    def _create_scheduler(self):
        self._scheduler = Scheduler(
            actors=[
                get_session_slot(handler, depot_path=self.depot.root)
                for handler in self._handler_dic.values()
            ],
            resources=self.resources,
            distributed_system=MultiProcAPI,
            stall_timeout=self.stall_timeout,
            task_timeout=self.task_timeout,
            verbose=self.debug,
        )

    def _get_reg_events(self, urls_to_register, urls_to_overwrite):
        out = []
        for url_dic, ovw in [(urls_to_register, False), (urls_to_overwrite, True)]:
            for handler, urls in (url_dic or {}).items():
                out.extend(_get_event_bunch(handler, urls, ovw))
                self.register_handler(handler)
        return out


def _get_event_bunch(handler: type[urh.ANY_HANDLER_T], urls, overwrite=False):
    part = partial(RegEvent, handler=handler.__name__, overwrite=overwrite)
    return map(part, map(handler.extend_link, urls))
