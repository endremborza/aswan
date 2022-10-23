from aswan.connection_session import RequestSession

from ..godel_src.handlers import AuthedProxy


def test_proxy_formatting(test_proxy):
    rs = RequestSession()

    ap = AuthedProxy()
    rs.start(ap)

    assert rs.driver.proxies
    assert ap.get_chrome_options()
