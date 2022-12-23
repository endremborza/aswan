from importlib import import_module
from typing import Dict, Iterable, Optional, Union

from . import url_handler as urh
from .connection_session import ConnectionSession
from .project import Project


def get_soup(
    url: str, params: Optional[dict] = None, browser=False, headless=True, headers=None
):
    cs = ConnectionSession(is_browser=browser, headless=headless)
    if headers and not browser:
        cs.session.driver.headers = headers
    out = cs.get_parsed_response(url, params=params)
    cs.stop()
    return out


def get_json(url: str, params: Optional[dict] = None):
    cs = ConnectionSession()
    return cs.get_parsed_response(url, urh.RequestJsonHandler(), params)


def run_simple_project(
    urls_for_handlers: Dict[urh.ANY_HANDLER_T, Iterable[str]],
    name: str,
    sync=False,
    remote: Optional[Union[str, bool]] = None,
):
    project = Project(name)

    for handler, urls in urls_for_handlers.items():
        project.register_handler(handler)
        assert not isinstance(urls, str), "set an iterable for urls, not str"
    project.register_module(import_module("__main__"))

    if remote:
        project.depot.setup()
        project.depot.pull(remote)

    project.run(urls_to_overwrite=urls_for_handlers, force_sync=sync)
    project.commit_current_run()

    if remote:
        project.depot.push(remote)
