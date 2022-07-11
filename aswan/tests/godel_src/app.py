import os
import sys
import time
from contextlib import contextmanager
from multiprocessing import Process
from typing import Optional

import requests
from flask import Flask, request, send_from_directory
from structlog import get_logger

logger = get_logger()

app = Flask(__name__, static_url_path="")
test_app_default_port = 5000
test_app_default_address = f"http://localhost:{test_app_default_port}"


@app.route("/test_page/<path:path>")
def send_js(path):
    return send_from_directory("test_pages", path)


@app.route("/test_param")
def get_param():
    return request.args.get("param")


@app.route("/test_post")
def get_post_param():
    return request.form.get("param")


class AppRunner:
    def __init__(self, port_no=test_app_default_port, verbose=True):
        self._port_no = port_no
        self.app_address = f"http://localhost:{port_no}"
        self._process: Optional[Process] = None
        self._verbose = verbose

    def start(self):
        self._process = Process(target=self._run_app)
        self._process.start()
        for i in range(10):
            try:
                requests.get(self.app_address, timeout=15)
                break
            except requests.exceptions.ConnectionError as e:
                if i > 5:
                    logger.exception(f"{type(e)} - {e}")
            time.sleep(0.2)
        return self

    def stop(self):
        self._process.kill()

    def _run_app(self):
        if not self._verbose:
            sys.stdout = open(os.devnull, "w")
            sys.stderr = open(os.devnull, "w")
        app.run(port=self._port_no)


@contextmanager
def test_app_context(port_no=test_app_default_port):

    _ar = AppRunner(port_no).start()
    yield
    _ar.stop()
