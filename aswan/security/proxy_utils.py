import zipfile
from typing import TYPE_CHECKING, Optional

from selenium import webdriver

if TYPE_CHECKING:
    from .proxy_base import ProxyAuth


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
        chrome_options.add_argument(
            f"--proxy-server={prefix}://{host}:{port_no}"
        )
        chrome_options.add_argument(
            f'--host-resolver-rules="MAP * ~NOTFOUND , EXCLUDE {host}"'
        )
    else:
        pluginfile = f"/tmp/{host}.zip"
        with zipfile.ZipFile(pluginfile, "w") as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr(
                "background.js",
                background_js % (host, port_no, auth.user, auth.password),
            )
        chrome_options.add_extension(pluginfile)
    return chrome_options
