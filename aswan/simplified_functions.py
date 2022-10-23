from typing import Optional

from .connection_session import ConnectionSession
from .url_handler import RequestJsonHandler


def get_soup(url: str, params: Optional[dict] = None, browser=False, headless=True):
    cs = ConnectionSession(is_browser=browser, headless=headless)
    out = cs.get_parsed_response(url, params=params)
    cs.stop()
    return out


def get_json(url: str, params: Optional[dict] = None):
    return ConnectionSession().get_parsed_response(url, RequestJsonHandler(), params)
