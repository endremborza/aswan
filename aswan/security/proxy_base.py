import json
import random
import time
from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional

from selenium.webdriver import ChromeOptions

from ..constants import CONFIG_PATH
from .proxy_utils import get_chrome_options


@dataclass
class ProxyAuth:
    user: str
    password: str


class ProxyBase:

    expiration_secs = 7 * 24 * 60 * 60
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
                f"{self.prefix}://{auth.user}:"
                f"{auth.password}@{host}:{self.port_no}"
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
            expired = (
                time.time() - ppath.lstat().st_mtime
            ) > self.expiration_secs
        except FileNotFoundError:
            expired = True

        if expired:
            host = self._load_host_list()
            ppath.write_text(json.dumps(host))

    def _get_path(self):
        proxy_name = type(self).__name__
        return CONFIG_PATH / f"{proxy_name}_hosts.json".lower()


class NoProxy(ProxyBase):
    def chrome_optins_from_host(self, host):
        return ChromeOptions()

    def rdict_from_host(self, host):
        return {}

    def _load_host_list(self) -> list:
        return [None]
