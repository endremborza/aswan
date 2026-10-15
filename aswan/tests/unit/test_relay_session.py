import json
import multiprocessing as mp
import time

import pytest
import requests

import aswan
from aswan.constants import (
    RELAY_CONTENT_K,
    RELAY_PORT,
    RELAY_REG_K,
    RELAY_URL_K,
    RELAY_URL_ROUTE,
)
from aswan._relay_session import RelaySession
from aswan.url_handler import RelayHandler

_URL = "url1"
_URL_SRC = "resp-txt"

_URL2 = "url2b"
_URL2_SRC = "resp-txt-b"

_REG_URL = "url2"
_REG_SRC = "reg-txt"


class WH(RelayHandler):
    pass


def test_simple(test_project: aswan.Project):
    test_project.register_handler(WH)

    proc = mp.Process(
        target=_relay_worker,
        args=({_URL: (_URL_SRC, [_REG_URL]), _REG_URL: (_REG_SRC, [])},),
    )
    proc.start()

    test_project.run({WH: [_URL]})
    proc.join()

    def _get_res():
        return sorted(
            (c.url, c.content)
            for c in test_project.depot.get_handler_events(WH, from_current=True)
        )

    assert _get_res() == [(_URL, _URL_SRC), (_REG_URL, _REG_SRC)]

    proc2 = mp.Process(target=_relay_worker, args=({_URL2: (_URL2_SRC, [])},))
    proc2.start()

    test_project.continue_run(urls_to_register={WH: [_URL2]})
    proc2.join()

    assert _get_res() == [(_URL, _URL_SRC), (_REG_URL, _REG_SRC), (_URL2, _URL2_SRC)]


class _Impatient(RelayHandler):
    relay_timeout = 0.2
    wait_time = 0.05

    def __init__(self):
        super().__init__()
        self.commands: list[str] = []

    def make_command(self, url: str):
        self.commands.append(url)


def test_response_arriving_after_a_timeout_is_kept():
    session = RelaySession()
    session.start(None)
    handler = _Impatient()
    try:
        with pytest.raises(ValueError):
            session.get_response_content(handler, _URL)
        late = {RELAY_CONTENT_K: _URL_SRC, RELAY_URL_K: _URL}
        requests.post(f"http://localhost:{RELAY_PORT}", json=late)
        assert json.loads(session.get_response_content(handler, _URL)) == late
    finally:
        session.stop()
    assert handler.commands == [_URL]


def _relay_worker(content_by_url: dict):
    """Mimics an external worker: poll for the pending url, post its content."""
    posted: set[str] = set()
    for _ in range(100):
        if posted >= content_by_url.keys():
            return
        time.sleep(0.2)
        try:
            url = requests.get(
                f"http://localhost:{RELAY_PORT}/{RELAY_URL_ROUTE}", timeout=1
            ).text
        except Exception:
            continue
        if (not url) or (url in posted) or (url not in content_by_url):
            continue
        content, register = content_by_url[url]
        requests.post(
            f"http://localhost:{RELAY_PORT}",
            json={RELAY_CONTENT_K: content, RELAY_URL_K: url, RELAY_REG_K: register},
        )
        posted.add(url)
