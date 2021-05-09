from .scheduler.resource import Resource, ResourceLimit


class UrlBaseConnection(Resource):
    actor_resource = False

    def __init__(self, url_root: str):
        self.url_root = url_root


class HeadlessBrowserResource(Resource):
    pass


class EagerBrowserResource(Resource):
    pass


class BrowserResource(Resource):
    pass


class ProxyResource(Resource):
    def __init__(self, proxy_kls):
        self.proxy_kls = proxy_kls


class Limit(ResourceLimit):
    pass
    # TODO
    # per machine / total
    # proxy total (per kind)
    # browser / machine (0 or more)
    # total thread per machine
    # url connection to url root - non actor specific
    # global limit vs machine limit vs url (special) limit
