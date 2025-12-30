# -*- coding: utf-8 -*-
import os

import pytest

from pymongosql.connection import Connection

# Silence SQLAlchemy 1.4 deprecation warnings in tests
os.environ.setdefault("SQLALCHEMY_SILENCE_UBER_WARNING", "1")

# Centralized test configuration sourced from environment to allow running tests
# against remote MongoDB (e.g. Atlas) or local test instance.
TEST_URI = os.environ.get("PYMONGOSQL_TEST_URI") or os.environ.get("MONGODB_URI")
TEST_DB = os.environ.get("PYMONGOSQL_TEST_DB", "test_db")


def make_conn(**kwargs):
    """Create a Connection using TEST_URI if provided, otherwise use a local default."""
    if TEST_URI:
        if "database" not in kwargs:
            kwargs["database"] = TEST_DB
        return Connection(host=TEST_URI, **kwargs)

    # Default local connection parameters
    defaults = {"host": "mongodb://testuser:testpass@localhost:27017/test_db?authSource=test_db", "database": "test_db"}
    for k, v in defaults.items():
        kwargs.setdefault(k, v)
    return Connection(**kwargs)


def make_superset_conn(**kwargs):
    """Create a superset-mode Connection using TEST_URI if provided, otherwise use a local default."""
    if TEST_URI:
        # Convert test URI to superset mode by adding ?mode=superset query parameter
        if "?" in TEST_URI:
            superset_uri = TEST_URI + "&mode=superset"
        else:
            superset_uri = TEST_URI + "?mode=superset"
        if "database" not in kwargs:
            kwargs["database"] = TEST_DB
        return Connection(host=superset_uri, **kwargs)

    # Default local connection parameters with superset mode
    defaults = {
        "host": "mongodb://testuser:testpass@localhost:27017/test_db?authSource=test_db&mode=superset",
        "database": "test_db",
    }
    for k, v in defaults.items():
        kwargs.setdefault(k, v)
    return Connection(**kwargs)


@pytest.fixture
def conn():
    """Yield a Connection instance configured via environment variables and tear it down after use."""
    connection = make_conn()
    try:
        yield connection
    finally:
        try:
            connection.close()
        except Exception:
            pass


@pytest.fixture
def superset_conn():
    """Yield a superset-mode Connection instance and tear it down after use."""
    connection = make_superset_conn()
    try:
        yield connection
    finally:
        try:
            connection.close()
        except Exception:
            pass


@pytest.fixture
def make_connection():
    """Provide the helper make_conn function to tests that need to create connections with custom args."""
    return make_conn


# SQLAlchemy version compatibility
try:
    import sqlalchemy
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    SQLALCHEMY_VERSION = tuple(map(int, sqlalchemy.__version__.split(".")[:2]))
    SQLALCHEMY_2X = SQLALCHEMY_VERSION >= (2, 0)
    HAS_SQLALCHEMY = True

    # Handle declarative base differences
    if SQLALCHEMY_2X:
        try:
            from sqlalchemy.orm import DeclarativeBase, Session

            class Base(DeclarativeBase):
                pass

        except ImportError:
            from sqlalchemy.ext.declarative import declarative_base

            Base = declarative_base()
            from sqlalchemy.orm import Session
    else:
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.orm import Session

        Base = declarative_base()

except ImportError:
    SQLALCHEMY_VERSION = None
    SQLALCHEMY_2X = False
    HAS_SQLALCHEMY = False
    Base = None
    Session = None

# SQLAlchemy fixtures for dialect testing
if HAS_SQLALCHEMY:

    @pytest.fixture
    def sqlalchemy_engine():
        """Provide a SQLAlchemy engine connected to MongoDB. The URI is taken from environment variables
        (PYMONGOSQL_TEST_URI or MONGODB_URI) or falls back to a sensible local default.
        """
        uri = os.environ.get("PYMONGOSQL_TEST_URI") or os.environ.get("MONGODB_URI") or TEST_URI
        db = os.environ.get("PYMONGOSQL_TEST_DB") or TEST_DB

        def _ensure_uri_has_db(uri_value: str, database: str) -> str:
            if not database:
                return uri_value
            idx = uri_value.find("://")
            if idx == -1:
                return uri_value
            rest = uri_value[idx + 3 :]
            if "/" in rest:
                after = rest.split("/", 1)[1]
                if after == "" or after.startswith("?"):
                    return uri_value.rstrip("/") + "/" + database
                return uri_value
            return uri_value.rstrip("/") + "/" + database

        if uri:
            uri_to_use = _ensure_uri_has_db(uri, db)
        else:
            uri_to_use = "mongodb://testuser:testpass@localhost:27017/test_db"

        engine = create_engine(uri_to_use)
        yield engine
        engine.dispose()

    @pytest.fixture
    def session_maker(sqlalchemy_engine):
        """Provide a SQLAlchemy session maker."""
        return sessionmaker(bind=sqlalchemy_engine)
