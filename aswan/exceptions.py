class AswanConnectionError(Exception):
    pass


class BrokenSessionError(Exception):
    pass


# backwards-compat alias
ConnectionError = AswanConnectionError
