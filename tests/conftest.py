# -*- coding: utf-8 -*-
import os

import pytest

from pymongosql.connection import Connection

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
def make_connection():
    """Provide the helper make_conn function to tests that need to create connections with custom args."""
    return make_conn
