import os
from pathlib import Path

from bs4 import BeautifulSoup

import aswan
from aswan.simplified_functions import get_json, get_soup, run_simple_project
from aswan.tests.godel_src.app import test_app_default_address


def test_get_soup(godel_test_app):
    soup = get_soup(f"{test_app_default_address}/test_page/godel_wiki.html")
    assert isinstance(soup, BeautifulSoup)
    assert soup.find("a").text == "axiomatic"


def test_get_json(godel_test_app):
    d = get_json(f"{test_app_default_address}/test_page/test_json.json")
    assert isinstance(d, dict)
    assert d["A"] == 10


def test_project_run(env_auth_id: str, godel_test_app, tmp_path: Path):

    os.environ["ASWAN_DEPOT_ROOT"] = tmp_path.as_posix()

    run_simple_project(
        {aswan.RequestHandler: [f"{test_app_default_address}/test_page/Axiom.html"]},
        name="simp-test",
        remote=env_auth_id,
        sync=True,
    )

    project = aswan.Project("simp-test", local_root=tmp_path / "other-thing")

    project.depot.pull(env_auth_id, True)
    project.depot.setup(True)

    axiom_res = next(project.handler_events(past_run_count=1))
    assert b"Axiom" in axiom_res.content
    assert "Axiom" in axiom_res.__repr__()
