# -*- coding: utf-8 -*-
import pytest
from pymongosql.connection import Connection
from pymongosql.cursor import Cursor
from pymongosql.error import OperationalError


class TestConnection:
    """Simplified test suite for Connection class - focuses on Connection-specific functionality"""

    def test_connection_init_no_defaults(self):
        """Initializing with no database should raise an error (enforced)"""
        with pytest.raises(OperationalError):
            Connection()

    def test_connection_init_with_basic_params(self):
        """Test connection initialization with basic parameters"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        assert conn.host == "mongodb://localhost:27017"
        assert conn.port == 27017
        assert conn.database_name == "test_db"
        assert conn.is_connected
        conn.close()

    def test_connection_with_connect_false(self):
        """Test connection with connect=False requires explicit database"""
        # Without explicit database, constructing should raise
        with pytest.raises(OperationalError):
            Connection(host="localhost", port=27017, connect=False)

        # With explicit database it should succeed
        conn = Connection(host="localhost", port=27017, connect=False, database="test_db")
        assert conn.host == "mongodb://localhost:27017"
        assert conn.port == 27017
        assert conn._client is not None
        conn.close()

    def test_connection_pymongo_parameters(self):
        """Test that PyMongo parameters are accepted when a database is provided"""
        # Provide explicit database to satisfy the enforced requirement
        conn = Connection(
            host="localhost",
            port=27017,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=10000,
            maxPoolSize=50,
            connect=False,  # Don't actually connect to avoid auth errors
            database="test_db",
        )
        assert conn.host == "mongodb://localhost:27017"
        assert conn.port == 27017
        conn.close()

    def test_connection_init_with_auth_username(self):
        """Test connection initialization with auth username"""
        conn = Connection(
            host="localhost",
            port=27017,
            database="test_db",
            username="testuser",
            password="testpass",
            authSource="test_db",
        )

        assert conn.database_name == "test_db"
        assert conn.is_connected
        conn.close()

    def test_cursor_creation(self):
        """Test cursor creation"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        cursor = conn.cursor()

        assert isinstance(cursor, Cursor)
        assert cursor._connection == conn
        conn.close()

    def test_context_manager(self):
        """Test connection as context manager"""
        conn = Connection(host="localhost", port=27017, database="test_db")

        with conn as connection:
            assert connection.is_connected
            assert connection == conn

        assert not conn.is_connected

    def test_context_manager_exception(self):
        """Test context manager with exception"""
        conn = Connection(host="localhost", port=27017, database="test_db")

        try:
            with conn as connection:
                assert connection.is_connected
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert not conn.is_connected

    def test_connection_string_representation(self):
        """Test string representation of connection"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        str_repr = str(conn)

        assert "localhost" in str_repr
        assert "27017" in str_repr
        assert "test_db" in str_repr
        conn.close()

    def test_disconnect_success(self):
        """Test successful disconnection"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        conn.disconnect()

        assert not conn.is_connected
        assert conn._client is None
        assert conn._database is None

    def test_close_method(self):
        """Test close method functionality"""
        conn = Connection(host="localhost", port=27017, database="test_db")

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
        conn = Connection(host="mongodb://localhost:27017/uri_db", database="explicit_db")
        assert conn.database is not None
        assert conn.database.name == "explicit_db"
        conn.close()

    def test_no_database_param_uses_client_default_database(self):
        """When no explicit database parameter is passed, use client's default from URI if present"""
        conn = Connection(host="mongodb://localhost:27017/default_db")
        assert conn.database is not None
        assert conn.database.name == "default_db"
        conn.close()
