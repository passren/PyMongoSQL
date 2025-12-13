# -*- coding: utf-8 -*-
import pytest
from pymongosql.connection import Connection
from pymongosql.cursor import Cursor


class TestConnection:
    """Test suite for Connection class"""

    def test_connection_init_no_defaults(self):
        """Test that connection doesn't accept empty parameters"""
        with pytest.raises(TypeError):
            Connection()

    def test_connection_init_with_params(self):
        """Test connection initialization with parameters"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        assert conn.host == "localhost"
        assert conn.port == 27017
        assert conn.database_name == "test_db"
        assert conn.username is None
        assert conn.password is None
        assert conn.is_connected

    def test_connection_auth_properties(self):
        """Test connection authentication properties"""
        conn = Connection(
            host="localhost",
            port=27017,
            database="test_db",
            username="testuser",
            password="testpass",
            auth_source="test_db",
        )
        # Test that auth properties are stored correctly
        assert conn.username == "testuser"
        assert conn.password == "testpass"
        assert conn.is_connected

    def test_connection_init_with_auth_full(self):
        """Test full connection with authentication"""
        conn = Connection(
            host="localhost",
            port=27017,
            database="test_db",
            username="testuser",
            password="testpass",
            auth_source="test_db",
        )
        assert conn.username == "testuser"
        assert conn.password == "testpass"
        assert conn.is_connected

    def test_connect_success(self):
        """Test successful database connection"""
        conn = Connection(host="localhost", port=27017, database="test_db")

        assert conn.is_connected
        assert conn._client is not None
        assert conn._database is not None
        assert conn.database_instance.name == "test_db"

    def test_connect_with_auth(self):
        """Test connection with authentication"""
        conn = Connection(
            host="localhost",
            port=27017,
            database="test_db",
            username="testuser",
            password="testpass",
            auth_source="test_db",
        )

        assert conn.is_connected
        assert conn.username == "testuser"
        assert conn.password == "testpass"

    def test_connect_behavior(self):
        """Test connection behavior (connects automatically in constructor)"""
        conn = Connection(host="localhost", port=27017, database="test_db")

        # Should be connected automatically
        assert conn.is_connected

    def test_disconnect_success(self):
        """Test successful disconnection"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        conn.disconnect()

        assert not conn.is_connected
        assert conn._client is None
        assert conn._database is None

    def test_disconnect_when_not_connected(self):
        """Test disconnecting when not connected"""
        conn = Connection(host="localhost", port=27017, database="test_db")
        # Disconnect first
        conn.disconnect()
        # Should not raise an error when disconnecting again
        conn.disconnect()

    def test_cursor_creation(self):
        """Test cursor creation"""
        conn = Connection(host="localhost", port=27017, database="test_db")

        cursor = conn.cursor()

        assert isinstance(cursor, Cursor)
        assert cursor._connection == conn

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
        assert "connected" in str_repr

    def test_connection_properties_when_connected(self):
        """Test connection properties when connected"""
        conn = Connection(host="localhost", port=27017, database="test_db")

        assert conn.client is not None
        assert conn.database_instance is not None
        assert conn.database_instance.name == "test_db"

    def test_real_database_operations(self):
        """Test actual database operations with real MongoDB"""
        conn = Connection(
            host="localhost",
            port=27017,
            database="test_db",
            username="testuser",
            password="testpass",
            auth_source="test_db",
        )

        # Clear existing test data to avoid duplicate key errors
        conn.database_instance.users.delete_many({})

        # Insert test data
        conn.database_instance.users.insert_many(
            [
                {"_id": "1", "name": "John", "age": 30},
                {"_id": "2", "name": "Jane", "age": 25},
            ]
        )

        # Query the data using MongoDB operations
        users = list(conn.database_instance.users.find({"age": {"$gte": 25}}))
        assert len(users) == 2

        # Test cursor with real SQL operations
        cursor = conn.cursor()
        result = cursor.execute("SELECT name FROM users WHERE age > 25")
        rows = result.fetchall()

        # Should find John (age 30)
        assert len(rows) >= 1
        names = [row.get("name") for row in rows]
        assert "John" in names

    def test_multiple_collections_access(self):
        """Test accessing multiple collections"""
        conn = Connection(
            host="localhost",
            port=27017,
            database="test_db",
            username="testuser",
            password="testpass",
            auth_source="test_db",
        )

        # Access different collections
        users_collection = conn.database_instance.users
        orders_collection = conn.database_instance.orders

        # Clear existing test data to avoid conflicts
        users_collection.delete_many({})
        orders_collection.delete_many({})

        # Insert data into different collections
        users_collection.insert_one({"name": "Alice"})
        orders_collection.insert_one({"user": "Alice", "amount": 100})

        # Verify data exists
        assert users_collection.count_documents({}) == 1
        assert orders_collection.count_documents({}) == 1
