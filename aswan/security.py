import base64
import json
import random
import time
import zipfile
from dataclasses import dataclass
from typing import Optional

from atqo import Capability
from selenium import webdriver
from selenium.webdriver import ChromeOptions

from .constants import CONFIG_PATH, ONE_YEAR


@dataclass
class ProxyAuth:
    user: str
    password: str


class ProxyBase:

    max_at_once = None
    expiration_secs = ONE_YEAR

    prefix = "http"
    port_no = 80

    def __init__(self):
        self._update_hosts()
        self._hosts = json.loads(self._get_path().read_text()) or [None]
        name = type(self).__name__
        self.res_id = f"proxy-resource-{name}"
        in_cap = {}
        if self.max_at_once:
            in_cap[self.res_id] = 1
        self.host = self.set_new_host()
        self.needs_auth = self.get_creds() is not None
        self.caps = [Capability(in_cap, name=f"{name}-pcap")]

    def get_creds(self) -> Optional[ProxyAuth]:
        """if authentication is needed"""
        pass

    def set_new_host(self):
        self.host = random.choice(self._hosts)
        return self.host

    def get_requests_dict(self):
        auth = self.get_creds()
        suffix = f"{self.host}:{self.port_no}"
        if auth is None:
            constring = f"{self.prefix}://{suffix}"
        else:
            constring = f"{self.prefix}://{auth.user}:{auth.password}@{suffix}"
        return {"http": constring, "https": constring}

    def get_chrome_options(self):
        return get_chrome_options(
            host=self.host,
            auth=self.get_creds(),
            prefix=self.prefix,
            port_no=self.port_no,
        )

    def _load_host_list(self) -> list:
        return []

    def _update_hosts(self):
        ppath = self._get_path()
        try:
            expired = (time.time() - ppath.lstat().st_mtime) > self.expiration_secs
        except FileNotFoundError:
            expired = True

        if expired:
            ppath.write_text(json.dumps(self._load_host_list()))

    def _get_path(self):
        file_path = CONFIG_PATH / f"{type(self).__name__}-hosts.json"
        file_path.parent.mkdir(exist_ok=True, parents=True)
        return file_path


class NoProxy(ProxyBase):
    def get_chrome_options(self):
        return ChromeOptions()

    def get_requests_dict(self):
        return {}


DEFAULT_PROXY = NoProxy


manifest_json = """
{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Chrome Proxy",
    "permissions": [
        "proxy",
        "tabs",
        "unlimitedStorage",
        "storage",
        "<all_urls>",
        "webRequest",
        "webRequestBlocking"
    ],
    "background": {
        "scripts": ["background.js"]
    },
    "minimum_chrome_version":"22.0.0"
}
"""

background_js = """
var config = {
        mode: "fixed_servers",
        rules: {
        singleProxy: {
            scheme: "http",
            host: "%s",
            port: parseInt(%s)
        },
        bypassList: ["localhost"]
        }
    };

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%s",
            password: "%s"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
            callbackFn,
            {urls: ["<all_urls>"]},
            ['blocking']
);
"""


def get_chrome_options(
    host, port_no, auth: Optional["ProxyAuth"] = None, prefix="http"
):
    chrome_options = webdriver.ChromeOptions()
    if auth is None:
        chrome_options.add_argument(f"--proxy-server={prefix}://{host}:{port_no}")
        chrome_options.add_argument(
            f'--host-resolver-rules="MAP * ~NOTFOUND , EXCLUDE {host}"'
        )
    else:
        ext_file = CONFIG_PATH / "proxy-exts" / f"{host}.zip"
        ext_file.parent.mkdir(exist_ok=True, parents=True)
        with zipfile.ZipFile(ext_file, "w") as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr(
                "background.js",
                background_js % (host, port_no, auth.user, auth.password),
            )
        chrome_options.add_encoded_extension(
            base64.b64encode(ext_file.read_bytes()).decode("UTF-8")
        )
    return chrome_options
