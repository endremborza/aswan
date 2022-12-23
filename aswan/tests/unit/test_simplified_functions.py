import os
from pathlib import Path

from bs4 import BeautifulSoup
from requests.utils import default_headers

import aswan
from aswan.constants import DEPOT_ROOT_ENV_VAR
from aswan.simplified_functions import get_json, get_soup, run_simple_project
from aswan.tests.godel_src.app import test_app_default_address


def test_get_soup(godel_test_app):
    h = default_headers()
    soup = get_soup(f"{test_app_default_address}/test_page/godel_wiki.html", headers=h)
    assert isinstance(soup, BeautifulSoup)
    assert soup.find("a").text == "axiomatic"


def test_get_json(godel_test_app):
    d = get_json(f"{test_app_default_address}/test_page/test_json.json")
    assert isinstance(d, dict)
    assert d["A"] == 10


def test_project_run(godel_env: str, tmp_path: Path):

    os.environ[DEPOT_ROOT_ENV_VAR] = tmp_path.as_posix()

    do_pushing = os.name != "nt"

    run_simple_project(
        {aswan.RequestHandler: [f"{test_app_default_address}/test_page/Axiom.html"]},
        name="simp-test",
        remote=do_pushing,
        sync=True,
    )

    if not do_pushing:  # pragma: no cover
        return
    project = aswan.Project("simp-test", local_root=tmp_path / "other-thing")

    project.depot.pull(godel_env, True)
    project.depot.setup(True)

    axiom_res = next(project.depot.get_handler_events(past_runs=1))
    assert b"Axiom" in axiom_res.content
    assert "Axiom" in axiom_res.__repr__()
