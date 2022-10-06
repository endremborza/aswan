from base64 import b64decode

from flask import Flask, request
from requests import get

proxy_app = Flask(__name__)
proxy_port = 8877
proxy_pw = "my-pw"
proxy_user = "my-user"


@proxy_app.route("/", defaults={"path": ""}, methods=["GET", "POST", "CONNECT"])
@proxy_app.route("/<path:path>", methods=["GET", "POST", "CONNECT"])
def proxy(path):  # pragma: no cover
    proper_host = request.headers.get("host")
    auth = request.headers.get("proxy-authorization")
    if auth:
        user, pw = b64decode(auth.split()[-1]).decode("utf-8").split(":")
        assert user == proxy_user and pw == proxy_pw
    proper_url = f"http://{proper_host}/{path}"
    if str(proxy_port) in proper_url:
        return b"OK"
    return get(proper_url).content
