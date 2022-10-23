# flake8: noqa
"""Data collection manager"""
from .connection_session import ConnectionSession
from .constants import ONE_DAY, ONE_HOUR, ONE_MINUTE, ONE_WEEK, ONE_YEAR, Statuses
from .depot import AswanDepot
from .exceptions import BrokenSessionError, ConnectionError
from .object_store import ObjectStore
from .project import Project
from .security import ProxyAuth, ProxyBase
from .simplified_functions import get_json, get_soup, run_simple_project
from .url_handler import (
    BrowserHandler,
    BrowserJsonHandler,
    BrowserSoupHandler,
    RequestHandler,
    RequestJsonHandler,
    RequestSoupHandler,
)

__version__ = "0.3.1"
