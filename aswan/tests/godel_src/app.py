from flask import Flask, request, send_from_directory
from structlog import get_logger

logger = get_logger()

test_app_default_port = 5000
test_app_default_address = f"http://localhost:{test_app_default_port}"


def godel_app_creator():  # pragma: no cover
    app = Flask(__name__, static_url_path="")
    app.route("/test_page/<path:path>")(send_file)
    app.route("/test_param")(get_param)
    app.route("/test_post")(get_post_param)
    return app


def send_file(path):  # pragma: no cover
    return send_from_directory("test_pages", path)


def get_param():  # pragma: no cover
    return request.args.get("param")


def get_post_param():  # pragma: no cover
    return request.form.get("param")
