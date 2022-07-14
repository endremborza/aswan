from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from aswan.project import Project

from aswan.tests.godel_src.app import AppRunner
from aswan.models import Base
from aswan.config_class import AswanConfig, DEFAULT_DIST_API
import pytest


@pytest.fixture(scope="session")
def godel_test_app(request):
    ar = AppRunner()
    ar.start()
    yield
    ar.stop()


@pytest.fixture(params=[DEFAULT_DIST_API], ids=[DEFAULT_DIST_API])
def dist_api_key(request):
    return request.param


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
def test_config():
    conf = AswanConfig("TC1")
    yield conf
    conf.purge(True)


@pytest.fixture
def test_project():
    proj = Project("TP1")
    yield proj
    proj.purge(True)


@pytest.fixture
def test_project2():
    proj = Project("TP2")
    yield proj
    proj.purge(True)
