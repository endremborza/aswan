from flask import Flask, request, send_from_directory
from structlog import get_logger

logger = get_logger()

godel_app = Flask(__name__, static_url_path="")
test_app_default_port = 5000
test_app_default_address = f"http://localhost:{test_app_default_port}"


@godel_app.route("/test_page/<path:path>")
def send_js(path):  # pragma: no cover
    return send_from_directory("test_pages", path)


@godel_app.route("/test_param")
def get_param():  # pragma: no cover
    return request.args.get("param")


@godel_app.route("/test_post")
def get_post_param():  # pragma: no cover
    return request.form.get("param")
