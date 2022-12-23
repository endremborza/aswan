import re
from pathlib import Path

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,"
    "application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:76.0)"
    " Gecko/20100101 Firefox/76.0",
}


DEPOT_ROOT_ENV_VAR = "ASWAN_DEPOT_ROOT"
DEFAULT_REMOTE_ENV_VAR = "ASWAN_REMOTE"
HEX_ENV = "ASWAN_AUTH_HEX"
PW_ENV = "ASWAN_AUTH_PASS"
DEFAULT_DEPOT_ROOT = Path.home() / "aswan-depots"
CONFIG_PATH = Path.home() / ".config" / "aswan"


url_root_regex = re.compile(r"^(?://|[^/]+)*/")

ONE_MINUTE = 60
ONE_HOUR = ONE_MINUTE * 60
ONE_DAY = ONE_HOUR * 24
ONE_WEEK = ONE_DAY * 7
ONE_YEAR = ONE_DAY * 365


class Statuses:

    TODO = "todo"
    PROCESSING = "processing"
    PROCESSED = "D"
    PERSISTENT_PROCESSED = "PP"
    CACHE_LOADED = "CL"
    PERSISTENT_CACHED = "PC"
    PARSING_ERROR = "PE"
    CONNECTION_ERROR = "CE"
    SESSION_BROKEN = "SB"


SUCCESS_STATUSES = [
    Statuses.PROCESSED,
    Statuses.CACHE_LOADED,
    Statuses.PERSISTENT_PROCESSED,
    Statuses.PERSISTENT_CACHED,
]
