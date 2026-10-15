"""URL-handler integration: cross-handler links and param URLs through the
real multi-process scheduler."""

import aswan
from aswan.tests.godel_src.app import test_app_default_address
from aswan.tests.godel_src.handlers import JS


class _Seed(aswan.RequestSoupHandler):
    """Routes only the page's first link to `JS`, keeping the queue short."""

    url_root = test_app_default_address

    def parse(self, soup):
        first = soup.find("a")
        if first is not None:
            self.register_links_to_handler([first], JS)
        return {"seed": "ok"}


def test_cross_handler_link_resolution(godel_test_app, test_project):
    test_project.register_handler(_Seed)
    test_project.register_handler(JS)
    test_project.run(
        urls_to_overwrite={_Seed: ["/test_page/godel_wiki.html"]},
    )

    detail_events = list(
        test_project.depot.get_handler_events(
            JS, only_successful=False, only_latest=False, from_current=True
        )
    )
    assert detail_events, "Seed should have registered one JS URL"
    # bs4 Tag href resolved against url_root
    for ev in detail_events:
        assert ev.url.startswith(test_app_default_address)


class _ParamHandler(aswan.RequestSoupHandler):
    """soup so parse() actually runs on the plain-text /test_param response."""

    url_root = test_app_default_address
    _called = 0

    def parse(self, soup):
        if self._called == 0:
            self._called = 1
            self.register_url_with_params({"param": "v"})
        return {"body": soup.get_text()}


def test_register_url_with_params_e2e(godel_test_app, test_project):
    test_project.register_handler(_ParamHandler)
    test_project.run(
        urls_to_overwrite={_ParamHandler: ["/test_param?param=seed"]},
    )

    events = list(
        test_project.depot.get_handler_events(
            _ParamHandler, only_successful=False, only_latest=False, from_current=True
        )
    )
    urls = {e.url for e in events}
    assert any("param=v" in u for u in urls)
