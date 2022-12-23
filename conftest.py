import os
import sys
import time
from multiprocessing import Process
from pathlib import Path
from typing import Optional

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from zimmauth import ZimmAuth
from zimmauth.core import LOCAL_HOST_NAMES_ENV_VAR
from zimmauth.test_core import TEST_USER, Server, private_key_path

from aswan import AswanDepot, Project
from aswan.constants import DEFAULT_REMOTE_ENV_VAR, DEPOT_ROOT_ENV_VAR, HEX_ENV, PW_ENV
from aswan.models import Base
from aswan.tests.godel_src.app import godel_app_creator, test_app_default_port
from aswan.tests.proxy_src import proxy_app_creator, proxy_port

_CNAME = "ssh-conn"


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
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def test_depot(tmp_path):
    depot = AswanDepot("TD1", tmp_path)
    depot.setup(True)
    yield depot
    depot.purge()


@pytest.fixture
def test_project():
    proj = Project("TP1")
    proj.depot.purge()
    yield proj
    # proj.depot.purge()


@pytest.fixture
def test_project2():
    proj = Project("TP2")
    yield proj
    proj.depot.purge()


@pytest.fixture(scope="session")
def test_proxy():

    ar = AppRunner(proxy_app_creator, proxy_port)
    ar.start()
    yield
    ar.stop()


@pytest.fixture
def env_auth_id(private_key_path: Path, tmp_path: Path):
    rem_path, local_path = tmp_path / "remote", tmp_path / "local"
    rem_path.mkdir()
    local_path.mkdir()
    test_dic = {
        "rsa-keys": {"rsa-key-name": private_key_path.read_text()},
        "ssh": {
            "ssh-name-1": {
                "host": Server.host,
                "port": 22,
                "user": TEST_USER,
                "rsa_key": "rsa-key-name",
            }
        },
        _CNAME: {"connection": "ssh-name-1", "path": rem_path.as_posix()},
    }
    pw = "AWPW7"
    os.environ[PW_ENV] = pw
    os.environ[HEX_ENV] = ZimmAuth.dumps_dict(test_dic, pw)
    os.environ[DEFAULT_REMOTE_ENV_VAR] = _CNAME
    os.environ[DEPOT_ROOT_ENV_VAR] = local_path.as_posix()
    os.environ[LOCAL_HOST_NAMES_ENV_VAR] = Server.host
    return _CNAME


@pytest.fixture
def godel_env(godel_test_app, env_auth_id):
    return env_auth_id
