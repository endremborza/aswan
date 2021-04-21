import logging
import multiprocessing
from asyncio import Future
from typing import TYPE_CHECKING, Type, Union

import ray
from ray.exceptions import RayError
from structlog import get_logger

from ..constants import DistApis

if TYPE_CHECKING:
    from .core import ActorFrameBase, SchedulerTask
    from .resource import ResourceBundle, ResourceLimitSet

logger = get_logger()


def get_dist_api(key) -> "DistAPI":
    if key == DistApis.RAY:
        return RayAPI
    elif key == DistApis.SYNC:
        return SyncAPI
    logger.warning(
        f"unknown distributed system: {key}, defaulting to sync api"
    )
    return SyncAPI


class DistAPI:
    def __init__(self, limit_set: "ResourceLimitSet"):
        self.resource_limit_set = limit_set

    @property
    def total_cpu_count(self):
        return multiprocessing.cpu_count()

    @property
    def exception(self):
        return Exception

    def join(self):
        """wait on all running tasks"""
        pass

    @staticmethod
    def kill(actor):
        actor.stop()

    @staticmethod
    def get_remote_actor(
        num_cpus: Union[int, float],
        resources: "ResourceBundle",
        actor_frame: Type["ActorFrameBase"],
    ) -> "RemoteActor":
        return RemoteActor(
            num_cpus=num_cpus, resources=resources, actor_frame=actor_frame
        )

    @staticmethod
    def get_future(actor, next_task: "SchedulerTask") -> Future:
        f = Future()
        f.set_result(actor.consume(next_task.argument))
        return f

    @staticmethod
    def parse_exception(e):
        return e


class RayAPI(DistAPI):
    def __init__(self, limit_set: "ResourceLimitSet"):
        super().__init__(limit_set)
        ray_specs = ray.init(
            resources=_limitset_to_ray_init(limit_set),
            log_to_driver=False,
            logging_level=logging.WARNING,
        )
        logger.info(f"ray dashboard: http://{ray_specs.get('webui_url')}")
        logger.info("launched ray with resources", **ray.cluster_resources())
        self._running = True

    @property
    def total_cpu_count(self):
        return ray.cluster_resources()["CPU"]

    @property
    def exception(self):
        return RayError

    def join(self):
        if self._running:
            ray.shutdown()
            self._running = False

    @staticmethod
    def kill(actor):
        ray.wait([actor.stop.remote()])
        ray.kill(actor)

    @staticmethod
    def get_remote_actor(
        num_cpus: Union[int, float],
        resources: "ResourceBundle",
        actor_frame: Type["ActorFrameBase"],
    ) -> "RemoteActor":
        return ray.remote(
            num_cpus=num_cpus,
            resources=_bundle_to_ray_actor(resources),
        )(actor_frame).remote(resources.resource_list)

    @staticmethod
    def get_future(actor, next_task: "SchedulerTask") -> Future:
        return actor.consume.remote(next_task.argument).as_future()

    @staticmethod
    def parse_exception(e):
        return e.cause_cls(e.traceback_str.strip().split("\n")[-1])


class SyncAPI(DistAPI):
    @property
    def total_cpu_count(self):
        return 1


class RemoteActor:
    def __init__(
        self,
        num_cpus: Union[int, float],
        resources: "ResourceBundle",
        actor_frame: Type["ActorFrameBase"],
    ):
        self._actor = actor_frame(resources.resource_list)

    def consume(self, arg):
        return self._actor.consume(arg)

    def stop(self):
        self._actor.stop()


def _bundle_to_ray_actor(resource_bundle: "ResourceBundle"):
    return {}  # TODO


def _limitset_to_ray_init(resource_lmit_set: "ResourceLimitSet"):
    return resource_lmit_set.to_dict(actor_specific=True)
