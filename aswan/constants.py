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

CONFIG_PATH = Path.home() / ".config" / "aswan"


url_root_regex = re.compile(r"^(?://|[^/]+)*/")

ONE_MINUTE = 60
ONE_HOUR = ONE_MINUTE * 60
ONE_DAY = ONE_HOUR * 24
ONE_WEEK = ONE_DAY * 7
ONE_YEAR = ONE_DAY * 365


class Statuses:

    TODO = "todo"
    EXPIRED = "expired"
    PROCESSING = "processing"
    PROCESSED = "processed"
    PARSING_ERROR = "parsing_error"
    CONNECTION_ERROR = "connection_error"
    SESSION_BROKEN = "session_broken"
