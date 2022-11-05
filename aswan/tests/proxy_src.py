from base64 import b64decode

from flask import Flask, Response, request
from requests import get

proxy_port = 8877
proxy_pw = "my-pw"
proxy_user = "my-user"


def proxy_app_creator():  # pragma: no cover
    app = Flask(__name__)
    app.route("/", defaults={"path": ""}, methods=["GET", "POST", "CONNECT"])(
        app.route("/<path:path>", methods=["GET", "POST", "CONNECT"])(proxy)
    )
    return app


def proxy(path):  # pragma: no cover
    proper_host = request.headers.get("host")
    auth = request.headers.get("proxy-authorization")
    if auth:
        user, pw = b64decode(auth.split()[-1]).decode("utf-8").split(":")
        assert user == proxy_user and pw == proxy_pw
    proper_url = f"http://{proper_host}/{path}"
    if str(proxy_port) in proper_url:
        return b"OK"
    resp = get(proper_url)
    return Response(resp.content, status=resp.status_code)
