import os
import sys
import time
from multiprocessing import Process
from typing import Optional

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from aswan import AswanDepot, Project
from aswan.models import Base
from aswan.tests.godel_src.app import godel_app, test_app_default_port
from aswan.tests.proxy_src import proxy_app, proxy_port


class AppRunner:
    def __init__(self, app, port_no, verbose=True):
        self.app = app
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
                    raise e
            time.sleep(0.2)
        return self

    def stop(self):
        self._process.kill()

    def _run_app(self):
        if not self._verbose:
            sys.stdout = open(os.devnull, "w")
            sys.stderr = open(os.devnull, "w")
        self.app.run(port=self._port_no)


@pytest.fixture(scope="session")
def godel_test_app(request):
    ar = AppRunner(godel_app, test_app_default_port, verbose=False)
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
    depot.setup()
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

    ar = AppRunner(proxy_app, proxy_port)
    ar.start()
    yield
    ar.stop()
