import json
import random
import time
from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, Type

from atqo import Capability
from selenium.webdriver import ChromeOptions

from ..constants import CONFIG_PATH, ONE_YEAR
from .proxy_utils import get_chrome_options


@dataclass
class ProxyAuth:
    user: str
    password: str


class ProxyBase:

    name = None
    max_at_once = None

    expiration_secs = ONE_YEAR
    max_num_at_once = float("inf")

    prefix = "http"
    port_no = 80

    def __init__(self):
        self._update_hosts()
        self._hosts = json.loads(self._get_path().read_text())
        self._served_hosts = 0

    def get_creds(self) -> Optional[ProxyAuth]:
        """if authentication is needed"""
        pass

    def get_new_host(self):
        self._served_hosts += 1
        return random.choice(self._hosts)

    def rdict_from_host(self, host):
        auth = self.get_creds()
        if auth is None:
            proxy_constring = f"{self.prefix}://{host}:{self.port_no}"
        else:
            proxy_constring = (
                f"{self.prefix}://{auth.user}:" f"{auth.password}@{host}:{self.port_no}"
            )
        return {"http": proxy_constring, "https": proxy_constring}

    def chrome_optins_from_host(self, host):
        return get_chrome_options(
            host=host,
            auth=self.get_creds(),
            prefix=self.prefix,
            port_no=self.port_no,
        )

    @abstractmethod
    def _load_host_list(self) -> list:
        pass

    def _update_hosts(self):
        ppath = self._get_path()
        try:
            expired = (time.time() - ppath.lstat().st_mtime) > self.expiration_secs
        except FileNotFoundError:
            expired = True

        if expired:
            host = self._load_host_list()
            ppath.write_text(json.dumps(host))

    def _get_path(self):
        proxy_name = type(self).__name__.lower()
        file_path = CONFIG_PATH / f"{proxy_name}_hosts.json"
        file_path.parent.mkdir(exist_ok=True, parents=True)
        return file_path


class NoProxy(ProxyBase):
    def chrome_optins_from_host(self, host):
        return ChromeOptions()

    def rdict_from_host(self, host):
        return {}

    def _load_host_list(self) -> list:
        return [None]


DEFAULT_PROXY = NoProxy


class ProxyData:
    def __init__(self, kls: Type[ProxyBase] = DEFAULT_PROXY) -> None:
        self.kls = kls
        self.name = kls.name or kls.__name__
        self.res_id = f"proxy-resource-{self.name}"
        self.limit = kls.max_at_once
        in_cap = {}
        if self.limit:
            in_cap[self.res_id] = 1
        self.cap = Capability(in_cap)

    def __repr__(self) -> str:
        return f"Proxy({self.name})"
