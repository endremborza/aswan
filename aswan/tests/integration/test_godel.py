import os
from collections import Counter
from functools import partial

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
    depot = test_project.depot
    _ghe = partial(depot.get_handler_events, from_current=True)

    assert next(_ghe(ghandlers.LinkRoot)).status == aswan.Statuses.PROCESSED

    def _get_found():
        return sorted([pcev.content["main"] for pcev in _ghe(ghandlers.GetMain)])

    assert _get_found() == ["Alonzo Church"]
    test_project.continue_run()
    assert _get_found() == ["Alonzo Church", "Entscheidungsproblem"]

    with pytest.raises(PermissionError):
        test_project.commit_current_run()
    test_project.depot.current.purge()

    test_project.run(
        urls_to_register={
            ghandlers.LinkRoot: ["/test_page/godel_wiki.html"],
            ghandlers.JS: ["/test_page/test_json.json"],
        }
    )

    assert _get_found() == ["Alonzo Church", "Entscheidungsproblem"]
    assert ["Alan_Turing"] == [
        pcev.url.split("/")[-1]
        for pcev in _ghe(ghandlers.GetMain, only_successful=False)
        if pcev.status == aswan.Statuses.CONNECTION_ERROR
    ]

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
            for pcev in depot.get_handler_events(
                ghandlers.Clicker, only_latest=False, past_runs=2
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
