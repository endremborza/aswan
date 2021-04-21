from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from aswan.tests.godel_src.app import AppRunner
from aswan.models import Base
from aswan.constants import DistApis

import pytest


@pytest.fixture(scope="session")
def godel_test_app(request):
    ar = AppRunner()
    ar.start()
    request.addfinalizer(ar.stop)


@pytest.fixture(params=[DistApis.default()], ids=[DistApis.default()])
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
