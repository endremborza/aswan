from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from aswan.tests.godel_src.app import AppRunner
from aswan.models import Base
from aswan.config_class import DEFAULT_DIST_API, DEFAULT_PROD_DIST_API
import pytest


@pytest.fixture(scope="session")
def godel_test_app(request):
    ar = AppRunner()
    ar.start()
    request.addfinalizer(ar.stop)


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
