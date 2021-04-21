from aswan.scheduler import Scheduler, SchedulerTask
from aswan.tests.integration.scheduler.actors_for_tests import (
    AddActor,
    NullActor,
)


def test_empty_scheduler(dist_api_key):
    scheduler = Scheduler(NullActor, distributed_system=dist_api_key)
    assert scheduler.is_idle
    assert scheduler.is_empty
    assert scheduler.queued_task_count == 0


def test_consuming(dist_api_key):
    scheduler = Scheduler(AddActor, distributed_system=dist_api_key)
    scheduler.refill_task_queue([SchedulerTask(argument=1)])
    scheduler.join()
    assert 2 in scheduler.get_processed_results()


def test_multiple_consuming(dist_api_key):
    scheduler = Scheduler(AddActor, distributed_system=dist_api_key)
    scheduler.refill_task_queue([SchedulerTask(argument=i) for i in range(10)])
    scheduler.join()
    results = list(scheduler.get_processed_results())
    for i in range(10):
        assert (i + 1) in results

    null_scheduler = Scheduler(NullActor, distributed_system=dist_api_key)
    null_scheduler.refill_task_queue(
        [SchedulerTask(argument=0) for _ in range(5)]
    )
    null_scheduler.join()
    for r in null_scheduler.get_processed_results():
        assert r is None


def test_error_handling(dist_api_key):
    # for ray too
    scheduler = Scheduler(AddActor, distributed_system=dist_api_key)
    scheduler.refill_task_queue([SchedulerTask(argument="cantadd")])
    scheduler.join()
    assert TypeError in map(type, scheduler.get_processed_results())


def test_restart_after_error(dist_api_key):
    scheduler = Scheduler(AddActor, distributed_system=dist_api_key)
    scheduler.refill_task_queue(
        [SchedulerTask(argument="cantadd") for _ in range(5)]
        + [SchedulerTask(argument=0)]
    )
    scheduler.join()
    result_list = list(scheduler.get_processed_results())
    assert TypeError in map(type, result_list)
    assert 1 in result_list
