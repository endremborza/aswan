import asyncio
import itertools
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from queue import Empty, Queue
from threading import Thread
from typing import Any, Dict, Iterable, List, Optional, Type

from structlog import get_logger

from .distributed_api import DistAPI, RemoteActor, get_dist_api
from .resource import Resource, ResourceBundle, ResourceLimit, ResourceLimitSet

logger = get_logger()
POISON_KEY = "0"  # just make sure it comes before any other
POISON_PILL = None
ALLOWED_CONSUMER_FAILS = 5


def _start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ActorListenBreaker(Exception):
    pass


class ActorPoisoned(ActorListenBreaker):
    pass


class NotEnoughResourcesToContinue(Exception):
    pass


class Scheduler:
    def __init__(
        self,
        actor_frame: Type["ActorFrameBase"],
        resource_limits: Optional[List[ResourceLimit]] = None,
        distributed_system: str = "sync",
        reorganize_after_every_task: bool = True,  # TODO overkill
    ):

        self._result_queue = Queue()
        self._active_async_tasks = set()

        self._loop = asyncio.new_event_loop()
        self._thread = Thread(
            target=_start_background_loop, args=(self._loop,), daemon=True
        )
        self._thread.start()
        _dist_api_klass = get_dist_api(distributed_system)

        self._frequent_reorg = reorganize_after_every_task
        self._resource_limit_set = ResourceLimitSet(resource_limits)
        self._dist_api: DistAPI = _dist_api_klass(self._resource_limit_set)
        self._actor_frame = actor_frame
        self._actor_sets: Dict[str, ActorSet] = {}
        self._task_queues: Dict[str, TaskQueue] = {}
        self._used_actor_resources = ResourceBundle()
        self._used_task_resources = ResourceBundle()

        self._total_possible_consumer_number = int(
            self._dist_api.total_cpu_count / actor_frame.cpu_needs
        )

    def __del__(self):
        try:
            self._dist_api.join()
        except AttributeError:
            pass

    @property
    def is_empty(self) -> bool:
        return self.is_idle and self._result_queue.empty()

    @property
    def is_idle(self) -> bool:
        return not self._active_async_tasks

    @property
    def queued_task_count(self):
        return sum([tq.queued_task_count for tq in self._task_queues.values()])

    def refill_task_queue(self, task_batch: Iterable["SchedulerTask"]):
        self._run(self._refill_task_queue(task_batch))

    def wait_until_n_tasks_remain(self, remaining_tasks: int = 0):
        self._run(self._await_until(remaining_tasks))

    def join(self):
        self.wait_until_n_tasks_remain(0)
        self._run(self._drain_all_actor_sets())
        try:
            self._run(asyncio.wait(self._all_actors))
        except AssertionError:
            pass
        self._run(self._cleanup())
        self._dist_api.join()

    def get_processed_results(self) -> Iterable:
        while True:
            try:
                yield self._result_queue.get(False)
            except Empty:
                break

    @property
    def _running_consumer_count(self):
        return sum(
            [aset.running_actor_count for aset in self._actor_sets.values()]
        )

    @property
    def _all_actors(self):
        return itertools.chain(
            *[aset.all_actors for aset in self._actor_sets.values()]
        )

    def _run(self, coro, wait=True):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if wait:
            fut.result()

    async def _refill_task_queue(self, task_batch: Iterable["SchedulerTask"]):
        for scheduler_task in task_batch:
            await self._add_task(scheduler_task)
        await self._reorganize_actors()

    async def _add_task(self, scheduler_task: "SchedulerTask"):
        coro = self._await_future_and_put_result_to_queue(scheduler_task)
        async_task = asyncio.create_task(coro)
        self._active_async_tasks.add(async_task)
        q = self._get_queue_for_new_task(scheduler_task)
        await q.put(scheduler_task)

    async def _await_future_and_put_result_to_queue(
        self, scheduler_task: "SchedulerTask"
    ):
        scheduler_task.set_future()
        task_result = await scheduler_task.future
        self._result_queue.put(task_result)

    def _get_queue_for_new_task(self, task: "SchedulerTask") -> asyncio.Queue:
        task_queue = self._task_queues.get(task.resource_bundle.key, None)
        if task_queue is not None:
            return task_queue.in_queue
        return self._process_new_bundle(task.resource_bundle).in_queue

    def _process_new_bundle(
        self, resource_bundle: ResourceBundle
    ) -> "TaskQueue":
        actor_subbundle = resource_bundle.actor_subbundle
        self._process_new_actor_bundle(actor_subbundle)

        new_task_queue = TaskQueue(resource_bundle)
        self._task_queues[resource_bundle.key] = new_task_queue
        for actor_set in self._actor_sets.values():
            if actor_set.resource_bundle >= actor_subbundle:
                actor_set.add_task_queue(new_task_queue)
        return new_task_queue

    def _process_new_actor_bundle(self, actor_bundle: ResourceBundle):
        bundle_key = actor_bundle.key
        if self._actor_sets.get(bundle_key, None) is not None:
            return
        new_actor_set = ActorSet(
            resource_bundle=actor_bundle,
            actor_frame=self._actor_frame,
            dist_api=self._dist_api,
        )
        self._actor_sets[bundle_key] = new_actor_set
        for task_queue in self._task_queues.values():
            if (
                new_actor_set.resource_bundle
                >= task_queue.resource_bundle.actor_subbundle
            ):
                new_actor_set.add_task_queue(task_queue)

    async def _reorganize_actors(self):
        running_actors = self._running_consumer_count
        for actor_set in self._actor_sets.values():
            if actor_set.all_queues_empty:
                n_drained = await actor_set.drain_to(0)
                self._used_actor_resources -= (
                    actor_set.resource_bundle * n_drained
                )
                running_actors -= n_drained
        for _ in range(running_actors, self._total_possible_consumer_number):
            top_actor_set = self._get_actor_set_in_biggest_need()
            if top_actor_set is not None:
                await top_actor_set.add_new_actor()
                self._used_actor_resources += top_actor_set.resource_bundle
                running_actors += 1
        if (running_actors == 0) and self.queued_task_count:
            await self._cleanup()
            await self._cancel_remaining_tasks()
            raise NotEnoughResourcesToContinue(
                f"{self.queued_task_count} remaining and no launchable actors"
            )

    def _get_actor_set_in_biggest_need(self):
        min_consumers = float("inf")
        out = None
        for aset in self._actor_sets.values():
            if not self._resource_limit_set.satisfied(
                aset.resource_bundle + self._used_actor_resources
            ):
                continue
            if (aset.queued_task_count > aset.running_actor_count) and (
                aset.running_actor_count < min_consumers
            ):
                min_consumers = aset.running_actor_count
                out = aset
        return out

    async def _await_until(self, remaining_tasks: int = 0):
        return_when = (
            "FIRST_COMPLETED"
            if (self._frequent_reorg or remaining_tasks) > 0
            else "ALL_COMPLETED"
        )
        while len(self._active_async_tasks) > remaining_tasks:
            done, _ = await asyncio.wait(
                self._active_async_tasks, return_when=return_when
            )
            self._active_async_tasks.difference_update(done)
            if self._frequent_reorg:
                await self._reorganize_actors()

        await self._reorganize_actors()

    async def _drain_all_actor_sets(self):
        for actor_set in self._actor_sets.values():
            await actor_set.drain_to(0)

    async def _cleanup(self):
        for atask in set(
            itertools.chain(
                *[
                    aset.tasks_waiting_for_new_addition_to_queue
                    for aset in self._actor_sets.values()
                ]
            )
        ):
            atask.cancel()

    async def _cancel_remaining_tasks(self):
        for atask in self._active_async_tasks:
            atask.cancel()


@dataclass
class ActorSet:

    resource_bundle: ResourceBundle
    actor_frame: Type["ActorFrameBase"]
    dist_api: DistAPI

    _actor_listnening_async_task_dict: Dict[str, asyncio.Task] = field(
        default_factory=dict, init=False
    )
    _poison_queue: asyncio.Queue = field(
        init=False, default_factory=asyncio.Queue
    )
    _async_queue_dict: Dict[str, asyncio.Queue] = field(
        init=False, default_factory=dict
    )
    _async_queue_get_task_dict: Dict[str, asyncio.Task] = field(
        init=False, default_factory=dict
    )
    _poisoning_done_future: asyncio.Future = field(
        init=False, default_factory=asyncio.Future
    )

    def __post_init__(self):

        self._async_queue_dict[POISON_KEY] = self._poison_queue
        self._async_queue_get_task_dict[POISON_KEY] = asyncio.create_task(
            self._poison_queue.get()
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}(resources={self.resource_bundle}, "
            f"queued_tasks={self.queued_task_count}, "
            f"running_actors={self.running_actor_count})"
        )

    @property
    def all_queues_empty(self):
        return not self.queued_task_count

    @property
    def queued_task_count(self):
        return sum([q.qsize() for q in self._async_queue_dict.values()]) + sum(
            [t.done() for t in self._async_queue_get_task_dict.values()]
        )

    @property
    def running_actor_count(self):
        return len(self._actor_listnening_async_task_dict)

    @property
    def all_actors(self):
        return self._actor_listnening_async_task_dict.values()

    @property
    def tasks_waiting_for_new_addition_to_queue(self):
        return self._async_queue_get_task_dict.values()

    async def drain_to(self, target_count: int) -> int:
        n = 0
        for _ in range(target_count, self.running_actor_count):
            n += 1
            await self._poison_queue.put(POISON_PILL)
            await self._poisoning_done_future
            self._poisoning_done_future = asyncio.Future()
        return n

    async def add_new_actor(self):
        remote_actor: RemoteActor = self.dist_api.get_remote_actor(
            num_cpus=self.actor_frame.cpu_needs,
            resources=self.resource_bundle,
            actor_frame=self.actor_frame,
        )

        listener_name = uuid.uuid1().hex
        coroutine = self._listen(
            remote_actor=remote_actor,
            name=listener_name,
        )
        task = asyncio.create_task(coroutine, name=listener_name)
        logger.info("adding consumer", **self._log_dict())
        self._actor_listnening_async_task_dict[listener_name] = task

    def add_task_queue(self, task_queue: "TaskQueue"):
        rkey = task_queue.resource_bundle.key
        self._async_queue_dict[rkey] = task_queue.in_queue
        self._async_queue_get_task_dict[rkey] = asyncio.create_task(
            self._async_queue_dict[rkey].get()
        )

    async def _listen(self, remote_actor: RemoteActor, name: str):
        logger.info(
            "consumer listening",
            actor=type(remote_actor).__name__,
            **self._log_dict(),
        )
        fails = 0
        while True:
            next_task = await self._get_next_task()
            try:
                fails = await self._process_task(
                    remote_actor, next_task, fails
                )
            except ActorListenBreaker as e:
                logger.info(
                    "stopping consumer",
                    reason=e,
                    actor=type(remote_actor).__name__,
                    **self._log_dict(),
                )
                self.dist_api.kill(remote_actor)
                del self._actor_listnening_async_task_dict[name]
                self._poisoning_done_future.set_result(True)
                if not isinstance(e, ActorPoisoned):
                    await self.add_new_actor()
                return

    async def _get_next_task(self) -> "SchedulerTask":
        while True:
            await asyncio.wait(
                self._async_queue_get_task_dict.values(),
                return_when="FIRST_COMPLETED",
            )
            for rkey, astask in sorted(
                self._async_queue_get_task_dict.items(),
                key=lambda kv: kv[0],
                reverse=True,
            ):
                if astask.done():
                    next_task = self._async_queue_get_task_dict.pop(
                        rkey
                    ).result()
                    self._async_queue_get_task_dict[
                        rkey
                    ] = asyncio.create_task(self._async_queue_dict[rkey].get())
                    return next_task

    async def _process_task(
        self,
        remote_actor: RemoteActor,
        next_task: "SchedulerTask",
        fails: int,
    ):
        if next_task is POISON_PILL:
            raise ActorPoisoned("poisoned")
        try:
            out = await self.dist_api.get_future(remote_actor, next_task)
            next_task.future.set_result(out)
            return 0
        except self.dist_api.exception as e:
            logger.warning(
                "Remote consumption error ",
                e=e,
                te=type(e),
                **self._log_dict(),
            )
            next_task.fail_count += 1
            if next_task.fail_count > next_task.allowed_fail_count:
                next_task.future.set_result(self.dist_api.parse_exception(e))
            else:
                await self._async_queue_dict[next_task.bundle_key].put(
                    next_task
                )
        if fails >= ALLOWED_CONSUMER_FAILS:
            raise ActorListenBreaker(f"{fails} number of fails reached")
        return fails + 1

    def _log_dict(self):
        return {
            "actor_set_resource_bundle": self.resource_bundle,
            "queued": self.queued_task_count,
            "actors_listening": self.running_actor_count,
        }


@dataclass
class TaskQueue:

    resource_bundle: ResourceBundle
    in_queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    @property
    def queued_task_count(self):
        return self.in_queue.qsize()


@dataclass
class SchedulerTask:
    resource_needs: List[Resource] = field(default_factory=list)
    argument: Any = None
    allowed_fail_count: int = 1
    future: Optional[asyncio.Future] = field(init=False, default=None)
    resource_bundle: ResourceBundle = field(init=False)
    fail_count: int = field(init=False, default=0)

    def __post_init__(self):
        self.resource_bundle = ResourceBundle(self.resource_needs)

    @property
    def bundle_key(self) -> str:
        return self.resource_bundle.key

    def set_future(self):
        self.future = asyncio.Future()


class ActorFrameBase(ABC):

    cpu_needs = 0.5

    def __init__(self, resource_needs: List[Resource]):
        self._resource_needs = resource_needs

    @abstractmethod
    def consume(self, next_task):
        pass  # pragma: no cover

    def stop(self):
        """if any cleanup needed"""
        pass
