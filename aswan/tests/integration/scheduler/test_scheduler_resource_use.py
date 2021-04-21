from pytest import raises

from aswan.scheduler import Resource, ResourceLimit, Scheduler, SchedulerTask
from aswan.scheduler.core import NotEnoughResourcesToContinue
from aswan.scheduler.resource import ResourceBundle
from aswan.tests.integration.scheduler.test_scheduler_basics import AddActor


class ResourceOne(Resource):
    pass


class ResourceTwo(Resource):
    pass


class NumberResource(Resource):
    def __init__(self, base: int):
        self.base = base


def test_insufficient_resources():
    limits = [
        ResourceLimit(ResourceOne, global_limit=0),
    ]
    scheduler = Scheduler(AddActor, resource_limits=limits)

    with raises(NotEnoughResourcesToContinue):
        scheduler.refill_task_queue(
            [SchedulerTask(argument=0, resource_needs=[ResourceOne()])]
        )
        scheduler.join()


def test_resource_based_reorganization():

    limits = [
        ResourceLimit(ResourceOne, global_limit=1),
        ResourceLimit(ResourceTwo, global_limit=1),
    ]

    scheduler = Scheduler(AddActor, resource_limits=limits)
    scheduler.refill_task_queue(
        [
            SchedulerTask(argument=i, resource_needs=[ResourceOne()])
            for i in range(5)
        ]
    )
    assert scheduler._used_actor_resources == ResourceBundle([ResourceOne()])
    scheduler.wait_until_n_tasks_remain(0)
    scheduler.refill_task_queue(
        [
            SchedulerTask(argument=i, resource_needs=[ResourceTwo()])
            for i in range(5)
        ]
    )
    assert scheduler._used_actor_resources == ResourceBundle([ResourceTwo()])
    scheduler.wait_until_n_tasks_remain(0)
    scheduler.join()


def test_mid_refill_reorganization():

    limits = [
        ResourceLimit(
            NumberResource,
            global_limit=1,
            target_attribute="base",
            limit_kind="nunique",
        ),
    ]

    scheduler = Scheduler(
        AddActor, resource_limits=limits, reorganize_after_every_task=True
    )
    scheduler.refill_task_queue(
        [
            SchedulerTask(argument=i, resource_needs=[NumberResource(1)])
            for i in range(5)
        ]
        + [
            SchedulerTask(argument=i, resource_needs=[NumberResource(2)])
            for i in range(5, 10)
        ]
    )
    scheduler.wait_until_n_tasks_remain(0)
    scheduler.join()
    assert list(range(1, 11)) == list(
        sorted(scheduler.get_processed_results())
    )
