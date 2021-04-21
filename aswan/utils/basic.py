import json
import re
from typing import Callable, List
from urllib.parse import ParseResult, parse_qsl, unquote, urlencode, urlparse

from structlog import get_logger

logger = get_logger()

url_root_rex = re.compile(r"(.*?://[a-zA-Z-\.]*)")


def run_and_log_functions(function_list: List[Callable], **kwargs):
    for fun in function_list:
        fun_name = fun.__name__
        logger.info(f"running function {fun_name}", **kwargs)
        res = fun()
        logger.info(f"function {fun_name} returned {res}", **kwargs)


def get_url_root(url: str):
    base = url_root_rex.findall(url)
    if base:
        return base[0]


def add_url_params(url, params):
    url = unquote(url)
    parsed_url = urlparse(url)
    get_args = parsed_url.query
    parsed_get_args = dict(parse_qsl(get_args))
    parsed_get_args.update(params)

    parsed_get_args.update(
        {
            k: json.dumps(v)
            for k, v in parsed_get_args.items()
            if isinstance(v, (bool, dict))
        }
    )

    encoded_get_args = urlencode(parsed_get_args, doseq=True)
    return ParseResult(
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        encoded_get_args,
        parsed_url.fragment,
    ).geturl()
