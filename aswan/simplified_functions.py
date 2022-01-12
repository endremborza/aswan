from typing import Optional

from .connection_session import ConnectionSession
from .url_handler import UrlHandler, UrlJsonHandler

_cs = ConnectionSession()


class _SH(UrlHandler):
    def parse_soup(self, soup):
        return soup


class _JH(UrlJsonHandler):
    def parse_json(self, d):
        return d


def get_soup(url: str, params: Optional[dict] = None, browser=False):
    _lcs = _cs
    if browser:
        _lcs = ConnectionSession(browser=True)
    out = _lcs.get_parsed_response(_SH(), url, params)
    if browser:
        _lcs.stop()
    return out


def get_json(url: str, params: Optional[dict] = None):
    return _cs.get_parsed_response(_JH(), url, params)
