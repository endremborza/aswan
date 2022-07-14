from ..security.proxy_base import ProxyBase
from ..t2_integrators import T2Integrator
from ..url_handler import UrlHandlerBase


def _is_subc(kls, base):
    try:
        return (base in kls.mro()) and (kls.__module__ != base.__module__)
    except AttributeError:
        return False


def is_proxy_base(kls):
    return _is_subc(kls, ProxyBase)


def is_handler(kls):
    return _is_subc(kls, UrlHandlerBase)


def is_t2_integrator(kls):
    return _is_subc(kls, T2Integrator)
