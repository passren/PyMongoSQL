# -*- coding: utf-8 -*-
from pymongosql.connection import Connection
from pymongosql.cursor import Cursor


class TestConnection:
    """Simplified test suite for Connection class - focuses on Connection-specific functionality"""

    def test_connection_init_no_defaults(self):
        """Test that connection can be initialized with no parameters (PyMongo compatible)"""
        conn = Connection()
        assert "mongodb://" in conn.host and "27017" in conn.host
        assert conn.port == 27017
        assert conn.database_name is None
        assert conn.is_connected
        conn.close()

    def test_connection_init_with_basic_params(self):
        """Test connection initialization with basic parameters"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        assert conn.host == "mongodb://localhost:27017"
        assert conn.port == 27017
        assert conn.database_name == "test_db"
        assert conn.is_connected
        conn.close()

    def test_connection_with_connect_false(self):
        """Test connection with connect=False (PyMongo compatibility)"""
        conn = Connection(host="localhost", port=27017, connect=False)
        assert conn.host == "mongodb://localhost:27017"
        assert conn.port == 27017
        # Should have client but not necessarily connected yet
        assert conn._client is not None
        conn.close()

    def test_connection_pymongo_parameters(self):
        """Test that PyMongo parameters are accepted"""
        # Test that we can pass PyMongo-style parameters without errors
        conn = Connection(
            host="localhost",
            port=27017,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=10000,
            maxPoolSize=50,
            connect=False,  # Don't actually connect to avoid auth errors
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
