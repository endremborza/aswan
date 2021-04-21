from selenium.webdriver import ChromeOptions

import aswan


class MyProxy(aswan.ProxyBase):
    name = "my-proxy"

    def chrome_optins_from_host(self, host):
        return ChromeOptions()

    def rdict_from_host(self, host):
        return {}

    def _load_host_list(self) -> list:
        return [None]
