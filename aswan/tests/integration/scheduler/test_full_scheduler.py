import time

from aswan.scheduler import ActorFrameBase, Scheduler, SchedulerTask
from aswan.scheduler.resource import ResourceLimit
from aswan.tests.integration.scheduler.test_scheduler_resources import (
    BigNumberCapability,
    PrimeBaseCapability,
)


def test_scheduler():

    resource_limits = [
        ResourceLimit(resource=BigNumberCapability, global_limit=1),
        ResourceLimit(
            resource=PrimeBaseCapability,
            global_limit=1,
            target_attribute="base",
            limit_kind="max_value_count",
        ),
    ]

    class TestActorFrame(ActorFrameBase):
        def consume(self, num):
            if num > 10:
                assert BigNumberCapability in map(type, self._resource_needs)
            time.sleep(0.01)
            return num + 1

    scheduler = Scheduler(
        actor_frame=TestActorFrame,
        resource_limits=resource_limits,
    )

    tasks = []
    test_batch1 = [5, 4, 15, 6, 7, 18, 0, 1, 9, 5, 15]  # 25, 35
    for x in test_batch1:
        resource_needs = []
        if x > 10:
            resource_needs.append(BigNumberCapability())
        if x > 20:
            resource_needs.append(BigNumberCapability())
        for pbase in [2, 3, 5]:
            if x % pbase == 0:
                resource_needs.append(PrimeBaseCapability(pbase))

        tasks.append(SchedulerTask(argument=x, resource_needs=resource_needs))

    scheduler.refill_task_queue(tasks)
    assert not scheduler.is_idle
    scheduler.join()
    assert sorted(scheduler.get_processed_results()) == sorted(
        map(lambda a: a + 1, test_batch1)
    )
    assert scheduler.is_idle
