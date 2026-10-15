import ctypes
import multiprocessing as mp
import os
import signal
import socket
import sys
import time

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from aswan import AswanDepot, Project
from aswan.models import Base
from aswan.tests.godel_src.app import godel_app_creator, test_app_default_port
from aswan.tests.proxy_src import proxy_app_creator, proxy_port

# Launch test servers with spawn: other tests build a Project (atqo's
# MultiProcAPI), leaving worker threads alive, and forking from a multi-threaded
# process is a deprecated, deadlock-prone pattern under Python 3.12+.
_SPAWN = mp.get_context("spawn")


def _die_with_parent():
    """Linux: SIGKILL this server if pytest dies, so an interrupted run can't
    leak a Flask process that keeps holding the port and poisons the next run."""
    if sys.platform != "linux":
        return
    try:
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(1, signal.SIGKILL)
    except Exception:
        pass


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _run_app(app_creator, port_no, verbose):
    _die_with_parent()
    if not verbose:
        sys.stdout = open(os.devnull, "w")
        sys.stderr = open(os.devnull, "w")
    app_creator().run(port=port_no)


class AppRunner:
    def __init__(self, app_creator, port_no, verbose=True):
        self.app_creator = app_creator
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
                assert self._process.is_alive()
                break
            except requests.exceptions.ConnectionError as e:
                if i > 5:
                    raise e
            time.sleep(0.2)
        return self

    def stop(self):
        self._process.kill()
        self._process.join()
        self._process.close()

    def _run_app(self):
        if not self._verbose:
            sys.stdout = open(os.devnull, "w")
            sys.stderr = open(os.devnull, "w")
        self.app_creator().run(port=self._port_no)


@pytest.fixture(scope="session")
def godel_test_app(request):
    ar = AppRunner(godel_app_creator, test_app_default_port, verbose=False)
    ar.start()
    yield
    ar.stop()


@pytest.fixture(scope="session")
def engine():
    return create_engine("sqlite://")


@pytest.fixture(scope="session")
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture
def dbsession(engine, tables):
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def test_depot(tmp_path):
    return AswanDepot("TD1", tmp_path).setup(True)


@pytest.fixture
def test_project(tmp_path):
    return Project("TP1", tmp_path)


@pytest.fixture(scope="session")
def test_proxy():
    ar = AppRunner(proxy_app_creator, proxy_port)
    ar.start()
    yield
    ar.stop()
