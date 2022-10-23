import os
from collections import Counter

import pytest

import aswan
import aswan.tests.godel_src.handlers as ghandlers


def test_godel(godel_test_app, test_proxy, env_auth_id, test_project: aswan.Project):
    """
    missing to test:
    - cookies
    """

    test_project.register_module(ghandlers)
    test_project.run(test_run=True, keep_running=False, force_sync=True)

    assert (
        next(test_project.handler_events(ghandlers.LinkRoot)).status
        == aswan.Statuses.PROCESSED
    )

    def _get_found():
        return [
            pcev.content["main"]
            for pcev in test_project.handler_events(ghandlers.GetMain)
        ]

    assert _get_found() == ["Alonzo Church"]
    test_project.continue_run()
    assert sorted(_get_found()) == ["Alonzo Church", "Entscheidungsproblem"]

    with pytest.raises(PermissionError):
        test_project.commit_current_run()
    test_project.cleanup_current_run()

    test_project.run(
        urls_to_register={
            ghandlers.LinkRoot: ["/test_page/godel_wiki.html"],
            ghandlers.JS: ["/test_page/test_json.json"],
        }
    )

    assert sorted(_get_found()) == ["Alonzo Church", "Entscheidungsproblem"]
    for pcev in test_project.handler_events(ghandlers.GetMain, only_successful=False):
        if pcev.status == aswan.Statuses.CONNECTION_ERROR:
            assert pcev.url.split("/")[-1] == "Alan_Turing"

    if os.name != "nt":
        test_project.depot.push(env_auth_id)
    test_project.commit_current_run()
    assert len(test_project.depot.get_all_run_ids()) == 1
    test_project.run()
    assert len(test_project.depot.get_all_run_ids()) == 1
    test_project.commit_current_run()
    test_project.run(
        urls_to_overwrite={
            ghandlers.GetMain: ["/test_page/Axiom.html"],
            ghandlers.Clicker: ["/test_page/jstest.html"],
        }
    )
    test_project.commit_current_run()
    assert len(test_project.depot.get_all_run_ids()) == 2

    urlcount = Counter(
        [
            pcev.url.split("/")[-1]
            for pcev in test_project.handler_events(
                ghandlers.Clicker, only_latest=False, past_run_count=2
            )
        ]
    )
    assert len(urlcount) == 3
    for k, v in urlcount.items():
        if k == "jstest.html":
            assert v == 2
        else:
            assert v == 1

    if os.name != "nt":
        test_project.depot.pull(env_auth_id)
        test_project.start_monitor_process()
        test_project.stop_monitor_process()
