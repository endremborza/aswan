import json
import logging
import subprocess
import threading
import time

from .constants import RELAY_PORT, RELAY_URL_K, RELAY_URL_ROUTE


class _RelayState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.current_url = ""
        self.responses: dict[str, bytes] = {}


class RelaySession:
    """Delegates collection to an external worker over an HTTP port.

    Per url: issue ``handler.make_command(url)``, wait for the worker to POST
    the result back, then issue ``handler.make_followup(response)``. A worker
    that prefers to pull can ``GET /<RELAY_URL_ROUTE>`` for the pending url.

    The Flask app runs in a daemon thread inside this worker process (shared
    in-memory state, no temp-file IPC, no nested multiprocessing).
    """

    def __init__(self, port: int = RELAY_PORT) -> None:
        self.port = port
        self.driver = None
        self._state = _RelayState()
        self._server = None
        self._thread: threading.Thread | None = None

    def start(self, _proxy) -> None:
        from werkzeug.serving import make_server

        if self._server is not None:
            return
        logging.getLogger("werkzeug").setLevel(logging.ERROR)
        self._server = make_server("0.0.0.0", self.port, _build_app(self._state))
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is None:
            return
        self._server.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None

    def get_response_content(self, handler, url: str) -> bytes:
        """A response that arrived after an earlier attempt timed out answers
        this attempt, without triggering the worker again."""
        with self._state.lock:
            self._state.current_url = url
            arrived = url in self._state.responses
        if not arrived:
            _run_command(handler.make_command(url))
        response = self._await_response(url, handler.relay_timeout, handler.wait_time)
        _run_command(handler.make_followup(response))
        return response

    def _await_response(self, url: str, timeout: float, wait_time: float) -> bytes:
        deadline = time.monotonic() + timeout
        while True:
            time.sleep(wait_time)
            with self._state.lock:
                if url in self._state.responses:
                    if self._state.current_url == url:
                        self._state.current_url = ""
                    return self._state.responses.pop(url)
            if time.monotonic() >= deadline:
                raise ValueError(f"relay response for {url} not received in {timeout}s")


def _run_command(command) -> None:
    if command:
        subprocess.run(command, check=False)


def _build_app(state: _RelayState):
    from flask import Flask, make_response, request
    from flask_cors import CORS

    app = Flask(__name__)
    CORS(app)

    @app.route(f"/{RELAY_URL_ROUTE}")
    def pending_url():
        with state.lock:
            return make_response(state.current_url)

    @app.route("/", methods=["POST"])
    def receive():
        body = request.get_data()
        try:
            url = json.loads(body)[RELAY_URL_K]
        except (ValueError, KeyError):
            return make_response("bad payload", 400)
        with state.lock:
            state.responses[url] = body
        return make_response("received")

    return app
