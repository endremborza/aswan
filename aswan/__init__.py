# flake8: noqa
"""Data collection manager"""
from .connection_session import ConnectionSession
from .constants import ONE_DAY, ONE_HOUR, ONE_MINUTE, ONE_WEEK, ONE_YEAR, Statuses
from .depot import AswanDepot, ParsedCollectionEvent
from .exceptions import BrokenSessionError, ConnectionError
from .monitor_app import app
from .object_store import ObjectStore
from .project import Project
from .security import ProxyAuth, ProxyBase
from .simplified_functions import get_json, get_soup, run_simple_project
from .url_handler import (
    ANY_HANDLER_T,
    BrowserHandler,
    BrowserJsonHandler,
    BrowserSoupHandler,
    RequestHandler,
    RequestJsonHandler,
    RequestSoupHandler,
)
from .utils import add_url_params

__version__ = "0.5.7"
