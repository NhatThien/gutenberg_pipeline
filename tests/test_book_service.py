import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from gutenberg_pipeline.database import Base
from gutenberg_pipeline.db_service import create_author, get_author


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:", echo=True)
    Base.metadata.create_all(engine)
    connection = engine.connect()
    transaction = connection.begin()
    db = scoped_session(sessionmaker(bind=engine))
    yield db
    transaction.rollback()
    connection.close()


def test_create_author(session):
    author = create_author(session, "toto")

    assert author.id is not None
    assert author.name == "toto"


def test_get_author(session):
    create_author(session, "toto")
    author = get_author(session, "toto")

    assert author is not None
    assert author.name == "toto"
