"""Focused unit tests for UrlHandlerBase plumbing."""

from bs4 import BeautifulSoup

import aswan
from aswan.models import RegEvent
from aswan.resources import CPU_BROWSER, CPU_SIMPLE, MCPU


class _H(aswan.RequestSoupHandler):
    url_root = "http://x.test"


def test_extend_link_with_bs4_tag():
    soup = BeautifulSoup('<a href="/foo">x</a>', "html.parser")
    assert _H.extend_link(soup.a) == "http://x.test/foo"


def test_extend_link_with_string():
    assert _H.extend_link("/bar") == "http://x.test/bar"


def test_register_links_defaults_to_self_handler():
    h = _H()
    h.register_links_to_handler(["/a", "/b"])
    out = h.pop_registered_links()
    assert all(isinstance(e, RegEvent) for e in out)
    assert {e.handler for e in out} == {"_H"}
    assert all(e.url.startswith("http://x.test/") for e in out)
    # pop_registered_links should drain
    assert h.pop_registered_links() == []


def test_register_url_with_params_empty_is_noop():
    h = _H()
    h.set_url("http://x.test/p")
    h.register_url_with_params({})  # nothing truthy
    h.register_url_with_params({"k": 0, "v": ""})  # all falsy
    assert h.pop_registered_links() == []


def test_register_url_with_params_appends():
    h = _H()
    h.set_url("http://x.test/p")
    h.register_url_with_params({"k": "v"})
    out = h.pop_registered_links()
    assert len(out) == 1
    assert "k=v" in out[0].url


def test_requirements_request_handler_uses_simple_cpu():
    h = _H()
    reqs = h.get_requirements()
    assert reqs[MCPU] == CPU_SIMPLE


def test_requirements_browser_handler_uses_browser_cpu():
    class B(aswan.BrowserSoupHandler):
        headless = True

    assert B().get_requirements()[MCPU] == CPU_BROWSER


def test_requirements_includes_max_in_parallel_slot():
    class P(aswan.RequestHandler):
        max_in_parallel = 2

    reqs = P().get_requirements()
    assert reqs["P"] == 1


def test_json_mixin_parses_bytes_to_dict():
    from aswan.url_handler import RequestJsonHandler

    h = RequestJsonHandler()
    out = h.pre_parse(b'{"a": 1}')
    assert out == {"a": 1}


def test_request_handler_default_session_broken():
    h = aswan.RequestHandler()
    assert h.is_session_broken(500) is True
    assert h.is_session_broken(404) is False
    assert h.is_session_broken(RuntimeError("x")) is True


def test_browser_handler_default_session_broken_false():
    class B(aswan.BrowserHandler):
        headless = True

    assert B().is_session_broken(RuntimeError("x")) is False
