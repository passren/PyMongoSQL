# -*- coding: utf-8 -*-
import pytest

from pymongosql.connection import Connection
from pymongosql.cursor import Cursor
from pymongosql.error import OperationalError
from tests.conftest import TEST_DB, TEST_URI


class TestConnection:
    """Simplified test suite for Connection class - focuses on Connection-specific functionality"""

    def test_connection_init_no_defaults(self):
        """Initializing with no database should raise an error (enforced)"""
        with pytest.raises(OperationalError):
            Connection()

    def test_connection_init_with_basic_params(self, conn):
        """Test connection initialization with basic parameters"""
        # When running against a remote URI we don't assert exact host string
        if TEST_URI:
            assert conn.is_connected
            assert conn.database_name == TEST_DB
        else:
            assert conn.host == "mongodb://localhost:27017"
            assert conn.port == 27017
            assert conn.database_name == "test_db"
            assert conn.is_connected
        conn.close()

    def test_connection_with_connect_false(self):
        """Test connection with connect=False requires explicit database"""
        # Without explicit database, constructing should raise
        with pytest.raises(OperationalError):
            # Explicitly request no connection attempt; without a database this should raise
            Connection(connect=False)

        # With explicit database it should succeed
        if TEST_URI:
            conn = Connection(host=TEST_URI, connect=False, database=TEST_DB)
        else:
            conn = Connection(host="localhost", port=27017, connect=False, database="test_db")

        # For connect=False we still have a client object created
        assert conn._client is not None
        conn.close()

    def test_connection_pymongo_parameters(self):
        """Test that PyMongo parameters are accepted when a database is provided"""
        # Provide explicit database to satisfy the enforced requirement
        if TEST_URI:
            conn = Connection(
                host=TEST_URI,
                port=27017,
                connectTimeoutMS=5000,
                serverSelectionTimeoutMS=10000,
                maxPoolSize=50,
                connect=False,  # Don't actually connect to avoid auth errors
                database=TEST_DB,
            )
        else:
            conn = Connection(
                host="localhost",
                port=27017,
                connectTimeoutMS=5000,
                serverSelectionTimeoutMS=10000,
                maxPoolSize=50,
                connect=False,  # Don't actually connect to avoid auth errors
                database="test_db",
            )
        if not TEST_URI:
            assert conn.host == "mongodb://localhost:27017"
            assert conn.port == 27017
        conn.close()

    def test_connection_init_with_auth_username(self, conn):
        """Test connection initialization with auth username"""
        # When running with TEST_URI the fixture provides a connection which may already contain credentials
        if TEST_URI:
            use_conn = conn
        else:
            use_conn = Connection(
                host="localhost",
                port=27017,
                database="test_db",
                username="testuser",
                password="testpass",
                authSource="test_db",
            )

        assert use_conn.database_name == (TEST_DB if TEST_URI else "test_db")
        assert use_conn.is_connected
        use_conn.close()

    def test_cursor_creation(self, conn):
        """Test cursor creation"""
        cursor = conn.cursor()

        assert isinstance(cursor, Cursor)
        assert cursor._connection == conn
        conn.close()

    def test_context_manager(self, conn):
        """Test connection as context manager"""

        with conn as connection:
            assert connection.is_connected
            assert connection == conn

        assert not conn.is_connected

    def test_context_manager_exception(self, conn):
        """Test context manager with exception"""

        try:
            with conn as connection:
                assert connection.is_connected
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert not conn.is_connected

    def test_connection_string_representation(self, conn):
        """Test string representation of connection"""
        str_repr = str(conn)

        # Ensure the representation contains something useful
        assert (TEST_DB in str_repr) or "test_db" in str_repr
        conn.close()

    def test_disconnect_success(self, conn):
        """Test successful disconnection"""
        conn.disconnect()

        assert not conn.is_connected
        assert conn._client is None
        assert conn._database is None

    def test_close_method(self, conn):
        """Test close method functionality"""

        # Verify connection is established
        assert conn.is_connected

        # Close connection
        conn.close()

        # Verify connection is closed
        assert not conn.is_connected
        assert conn._client is None
        assert conn._database is None

    def test_explicit_database_param_overrides_uri_default(self):
        """Explicit database parameter should take precedence over URI default"""
        # Test that explicit database parameter overrides URI default
        if TEST_URI:
            # Construct a URI with an explicit database path
            conn = Connection(host=f"{TEST_URI}", database="explicit_db")
        else:
            conn = Connection(host="mongodb://localhost:27017/uri_db", database="explicit_db")
        assert conn.database is not None
        assert conn.database.name == "explicit_db"
        conn.close()

    def test_no_database_param_uses_client_default_database(self):
        """When no explicit database parameter is passed, use client's default from URI if present"""
        if TEST_URI:
            conn = Connection(host=f"{TEST_URI}")
        else:
            conn = Connection(host="mongodb://localhost:27017/test_db")
        assert conn.database is not None
        assert conn.database.name == "test_db"
        conn.close()

    def test_connection_string_with_mode_query_param(self):
        """Test that connection string with ?mode parameter is parsed correctly"""
        if TEST_URI:
            # Test with mode parameter in query string
            test_url = f"{TEST_URI.rstrip('&')}&mode=superset"
        else:
            test_url = "mongodb://localhost:27017/test_db?mode=superset"

        conn = Connection(host=test_url)
        assert conn.mode == "superset"
        assert conn.database_name == "test_db"
        conn.close()
