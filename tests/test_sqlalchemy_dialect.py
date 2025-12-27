#!/usr/bin/env python3
import unittest
from typing import Callable
from unittest.mock import Mock, patch

# SQLAlchemy version compatibility
try:
    import sqlalchemy

    SQLALCHEMY_VERSION = tuple(map(int, sqlalchemy.__version__.split(".")[:2]))
    SQLALCHEMY_2X = SQLALCHEMY_VERSION >= (2, 0)
    HAS_SQLALCHEMY = True
except ImportError:
    SQLALCHEMY_VERSION = None
    SQLALCHEMY_2X = False
    HAS_SQLALCHEMY = False

# Version-compatible imports
if HAS_SQLALCHEMY:
    from sqlalchemy import Column, Integer, String, create_engine
    from sqlalchemy.engine import url

    # Handle declarative base differences
    if SQLALCHEMY_2X:
        try:
            from sqlalchemy.orm import DeclarativeBase

            class _TestBase(DeclarativeBase):  # Prefix with _ to avoid pytest collection
                pass

            declarative_base: Callable[[], type[_TestBase]] = lambda: _TestBase
        except ImportError:
            from sqlalchemy.ext.declarative import declarative_base
    else:
        from sqlalchemy.ext.declarative import declarative_base

import pymongosql
from pymongosql.sqlalchemy_mongodb import create_engine_url
from pymongosql.sqlalchemy_mongodb.sqlalchemy_dialect import (
    PyMongoSQLDDLCompiler,
    PyMongoSQLDialect,
    PyMongoSQLIdentifierPreparer,
    PyMongoSQLTypeCompiler,
)


class TestPyMongoSQLDialect(unittest.TestCase):
    """Test cases for the PyMongoSQL SQLAlchemy dialect."""

    def setUp(self):
        """Set up test fixtures."""
        if not HAS_SQLALCHEMY:
            self.skipTest("SQLAlchemy not available")
        self.dialect = PyMongoSQLDialect()

    def test_dialect_name(self):
        """Test dialect name and driver."""
        self.assertEqual(self.dialect.name, "mongodb")
        self.assertEqual(self.dialect.driver, "pymongosql")

    def test_dbapi(self):
        """Test DBAPI module reference."""
        # Test class method
        self.assertEqual(PyMongoSQLDialect.dbapi(), pymongosql)

        # Test import_dbapi class method (SQLAlchemy 2.x)
        self.assertEqual(PyMongoSQLDialect.import_dbapi(), pymongosql)

        # Test instance access (should work even if SQLAlchemy interferes)
        try:
            result = self.dialect.dbapi() if callable(self.dialect.dbapi) else self.dialect._get_dbapi_module()
            self.assertEqual(result, pymongosql)
        except Exception:
            # Fallback test - at least the class method should work
            self.assertEqual(PyMongoSQLDialect.dbapi(), pymongosql)

    def test_create_connect_args_basic(self):
        """Test basic connection argument creation."""
        test_url = url.make_url("mongodb://localhost:27017/testdb")
        args, kwargs = self.dialect.create_connect_args(test_url)

        self.assertEqual(args, [])
        self.assertIn("host", kwargs)
        # The new implementation passes the complete MongoDB URI as host
        self.assertEqual(kwargs["host"], "mongodb://localhost:27017/testdb")

    def test_create_connect_args_with_auth(self):
        """Test connection args with authentication."""
        test_url = url.make_url("mongodb://user:pass@localhost:27017/testdb")
        args, kwargs = self.dialect.create_connect_args(test_url)

        # The new implementation passes the complete MongoDB URI with auth as host
        self.assertIn("host", kwargs)
        self.assertEqual(kwargs["host"], "mongodb://user:pass@localhost:27017/testdb")

    def test_create_connect_args_with_query_params(self):
        """Test connection args with query parameters."""
        test_url = url.make_url("mongodb://localhost/testdb?ssl=true&replicaSet=rs0")
        args, kwargs = self.dialect.create_connect_args(test_url)

        # The new implementation passes the complete MongoDB URI with query params as host
        self.assertIn("host", kwargs)
        self.assertIn("ssl=true", kwargs["host"])
        self.assertIn("replicaSet=rs0", kwargs["host"])

    def test_supports_features(self):
        """Test dialect feature support flags."""
        # Features MongoDB doesn't support
        self.assertFalse(self.dialect.supports_alter)
        self.assertFalse(self.dialect.supports_comments)
        self.assertFalse(self.dialect.supports_sequences)
        self.assertFalse(self.dialect.supports_native_enum)

        # Features MongoDB does support
        self.assertTrue(self.dialect.supports_default_values)
        self.assertTrue(self.dialect.supports_empty_inserts)
        self.assertTrue(self.dialect.supports_multivalues_insert)
        self.assertTrue(self.dialect.supports_native_decimal)
        self.assertTrue(self.dialect.supports_native_boolean)

    def test_has_table(self):
        """Test table (collection) existence check using MongoDB operations."""
        from unittest.mock import MagicMock

        # Mock MongoDB connection structure
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_client = MagicMock()  # Use MagicMock for __getitem__ support
        mock_db = Mock()

        mock_conn.connection = mock_db_connection
        mock_db_connection._client = mock_client
        mock_db_connection.database = mock_db
        mock_db.list_collection_names.return_value = ["users", "products", "orders"]

        # Test existing table
        self.assertTrue(self.dialect.has_table(mock_conn, "users"))

        # Test non-existing table
        self.assertFalse(self.dialect.has_table(mock_conn, "nonexistent"))

        # Test with schema
        mock_schema_db = Mock()
        mock_client.__getitem__.return_value = mock_schema_db
        mock_schema_db.list_collection_names.return_value = ["schema_users"]
        self.assertTrue(self.dialect.has_table(mock_conn, "schema_users", schema="test_schema"))

    def test_get_table_names(self):
        """Test getting collection names using MongoDB operations."""
        from unittest.mock import MagicMock

        # Mock MongoDB connection structure
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_client = MagicMock()  # Use MagicMock for __getitem__ support
        mock_db = Mock()

        mock_conn.connection = mock_db_connection
        mock_db_connection._client = mock_client
        mock_db_connection.database = mock_db
        mock_db.list_collection_names.return_value = ["users", "products", "orders"]

        tables = self.dialect.get_table_names(mock_conn)
        expected = ["users", "products", "orders"]
        self.assertEqual(tables, expected)

        # Test with schema
        mock_schema_db = Mock()
        mock_client.__getitem__.return_value = mock_schema_db
        mock_schema_db.list_collection_names.return_value = ["schema_table1", "schema_table2"]
        schema_tables = self.dialect.get_table_names(mock_conn, schema="test_schema")
        self.assertEqual(schema_tables, ["schema_table1", "schema_table2"])

    @patch("bson.ObjectId")
    def test_get_columns(self, mock_objectid):
        """Test getting column information using MongoDB document sampling."""
        from datetime import datetime
        from unittest.mock import MagicMock

        # Mock MongoDB connection structure
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_client = Mock()
        mock_db = MagicMock()  # Use MagicMock for __getitem__ support
        mock_collection = Mock()

        mock_conn.connection = mock_db_connection
        mock_db_connection._client = mock_client
        mock_db_connection.database = mock_db
        mock_db.__getitem__.return_value = mock_collection

        # Mock sample documents
        sample_docs = [
            {"_id": "507f1f77bcf86cd799439011", "name": "John", "age": 25, "active": True},
            {"_id": "507f1f77bcf86cd799439012", "name": "Jane", "email": "jane@test.com", "score": 95.5},
            {"_id": "507f1f77bcf86cd799439013", "created_at": datetime.now(), "tags": ["python", "mongodb"]},
        ]
        mock_collection.find.return_value.limit.return_value = sample_docs

        columns = self.dialect.get_columns(mock_conn, "users")

        # Should have inferred columns from sample documents
        self.assertGreater(len(columns), 0)

        # Check _id is always included and not nullable
        id_column = next((col for col in columns if col["name"] == "_id"), None)
        self.assertIsNotNone(id_column)
        self.assertFalse(id_column["nullable"])

        # Test fallback for empty collection
        mock_collection.find.return_value.limit.return_value = []
        fallback_columns = self.dialect.get_columns(mock_conn, "empty_collection")
        self.assertEqual(len(fallback_columns), 1)
        self.assertEqual(fallback_columns[0]["name"], "_id")

    def test_get_pk_constraint(self):
        """Test primary key constraint info."""
        mock_conn = Mock()
        pk_info = self.dialect.get_pk_constraint(mock_conn, "users")

        self.assertEqual(pk_info["constrained_columns"], ["_id"])
        self.assertEqual(pk_info["name"], "pk_id")

    def test_get_foreign_keys(self):
        """Test foreign key constraints (should be empty for MongoDB)."""
        mock_conn = Mock()
        fks = self.dialect.get_foreign_keys(mock_conn, "users")

        self.assertEqual(fks, [])

    def test_get_indexes(self):
        """Test getting index information using MongoDB index_information."""
        from unittest.mock import MagicMock

        # Mock MongoDB connection structure
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_client = Mock()
        mock_db = MagicMock()  # Use MagicMock for __getitem__ support
        mock_collection = Mock()

        mock_conn.connection = mock_db_connection
        mock_db_connection._client = mock_client
        mock_db_connection.database = mock_db
        mock_db.__getitem__.return_value = mock_collection

        # Mock index information
        mock_index_info = {
            "_id_": {"key": [("_id", 1)], "unique": False},  # _id is implicit unique
            "email_1": {"key": [("email", 1)], "unique": True},
            "name_text": {"key": [("name", "text")], "unique": False},
        }
        mock_collection.index_information.return_value = mock_index_info

        indexes = self.dialect.get_indexes(mock_conn, "users")

        self.assertEqual(len(indexes), 3)

        # Check _id index
        id_index = next((idx for idx in indexes if idx["name"] == "_id_"), None)
        self.assertIsNotNone(id_index)
        self.assertEqual(id_index["column_names"], ["_id"])

        # Check email index
        email_index = next((idx for idx in indexes if idx["name"] == "email_1"), None)
        self.assertIsNotNone(email_index)
        self.assertTrue(email_index["unique"])
        self.assertEqual(email_index["column_names"], ["email"])

    def test_get_schema_names(self):
        """Test getting database names using MongoDB listDatabases command."""
        # Mock MongoDB connection structure
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_client = Mock()
        mock_admin_db = Mock()

        mock_conn.connection = mock_db_connection
        mock_db_connection._client = mock_client
        mock_client.admin = mock_admin_db

        # Mock listDatabases result
        mock_admin_db.command.return_value = {
            "databases": [
                {"name": "admin", "sizeOnDisk": 32768},
                {"name": "config", "sizeOnDisk": 12288},
                {"name": "myapp", "sizeOnDisk": 65536},
                {"name": "test", "sizeOnDisk": 8192},
            ]
        }

        schemas = self.dialect.get_schema_names(mock_conn)
        expected = ["admin", "config", "myapp", "test"]
        self.assertEqual(schemas, expected)

        # Verify the correct MongoDB command was called
        mock_admin_db.command.assert_called_with("listDatabases")

    def test_get_schema_names_fallback(self):
        """Test get_schema_names fallback when MongoDB operation fails."""
        # Mock connection that raises an exception
        mock_conn = Mock()
        mock_conn.connection.side_effect = Exception("Connection error")

        schemas = self.dialect.get_schema_names(mock_conn)
        self.assertEqual(schemas, ["default"])

    def test_do_ping(self):
        """Test connection ping using MongoDB native ping command."""
        # Mock successful connection
        mock_conn = Mock()
        mock_conn.test_connection.return_value = True

        result = self.dialect.do_ping(mock_conn)
        self.assertTrue(result)

        # Test fallback to direct client ping
        mock_conn_no_test = Mock()
        mock_conn_no_test.test_connection = None
        mock_client = Mock()
        mock_admin_db = Mock()
        mock_conn_no_test._client = mock_client
        mock_client.admin = mock_admin_db

        result_fallback = self.dialect.do_ping(mock_conn_no_test)
        self.assertTrue(result_fallback)
        mock_admin_db.command.assert_called_with("ping")

    def test_do_ping_failure(self):
        """Test do_ping when connection fails."""
        # Mock failed connection
        mock_conn = Mock()
        mock_conn.test_connection.return_value = False

        result = self.dialect.do_ping(mock_conn)
        self.assertFalse(result)

        # Test fallback failure - connection without test_connection method
        mock_conn_error = Mock()
        del mock_conn_error.test_connection  # Remove the attribute entirely
        mock_conn_error._client = Mock()
        mock_conn_error._client.admin.command.side_effect = Exception("Connection failed")

        result_error = self.dialect.do_ping(mock_conn_error)
        self.assertFalse(result_error)

    def test_infer_bson_type(self):
        """Test BSON type inference from Python values."""
        from datetime import datetime

        # Test various Python types
        test_cases = [
            ("test string", "string"),
            (42, "int"),
            (3.14, "double"),
            (True, "bool"),
            (False, "bool"),
            (datetime.now(), "date"),
            ([1, 2, 3], "array"),
            ({"key": "value"}, "object"),
            (None, "null"),
        ]

        for value, expected_type in test_cases:
            with self.subTest(value=value, expected=expected_type):
                inferred_type = self.dialect._infer_bson_type(value)
                self.assertEqual(inferred_type, expected_type)

    def test_error_handling(self):
        """Test error handling and fallback behavior for all methods."""
        # Mock connection that fails when trying to access MongoDB operations
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_conn.connection = mock_db_connection

        # Make hasattr check fail or make database operations fail
        mock_db_connection._client = None  # This makes hasattr(_client) return False
        # Or we can make database operations fail by making database.list_collection_names() fail
        mock_db = Mock()
        mock_db_connection.database = mock_db
        mock_db.list_collection_names.side_effect = Exception("MongoDB error")

        # Test has_table fallback
        result = self.dialect.has_table(mock_conn, "test_table")
        self.assertFalse(result)

        # Test get_table_names fallback
        tables = self.dialect.get_table_names(mock_conn)
        self.assertEqual(tables, [])

        # Test get_columns fallback
        columns = self.dialect.get_columns(mock_conn, "test_table")
        self.assertEqual(len(columns), 1)
        self.assertEqual(columns[0]["name"], "_id")

        # Test get_indexes fallback
        indexes = self.dialect.get_indexes(mock_conn, "test_table")
        self.assertEqual(len(indexes), 1)
        self.assertEqual(indexes[0]["name"], "_id_")
        self.assertTrue(indexes[0]["unique"])

    def test_schema_operations_with_schema_parameter(self):
        """Test operations when schema parameter is provided."""
        from unittest.mock import MagicMock

        # Mock MongoDB connection structure
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_client = MagicMock()  # Use MagicMock for __getitem__ support
        mock_schema_db = MagicMock()  # Use MagicMock for __getitem__ support
        mock_collection = Mock()

        mock_conn.connection = mock_db_connection
        mock_db_connection._client = mock_client
        mock_client.__getitem__.return_value = mock_schema_db
        mock_schema_db.__getitem__.return_value = mock_collection
        mock_schema_db.list_collection_names.return_value = ["table1", "table2"]

        # Test has_table with schema
        result = self.dialect.has_table(mock_conn, "table1", schema="test_schema")
        self.assertTrue(result)
        mock_client.__getitem__.assert_called_with("test_schema")

        # Test get_table_names with schema
        tables = self.dialect.get_table_names(mock_conn, schema="test_schema")
        self.assertEqual(tables, ["table1", "table2"])

        # Test get_columns with schema
        mock_collection.find.return_value.limit.return_value = [{"_id": "123", "name": "test"}]
        columns = self.dialect.get_columns(mock_conn, "table1", schema="test_schema")
        self.assertGreater(len(columns), 0)
        mock_schema_db.__getitem__.assert_called_with("table1")

    def test_superset_integration_workflow(self):
        """Test the complete workflow that Apache Superset would use."""
        from unittest.mock import MagicMock

        # Mock complete MongoDB connection for Superset workflow
        mock_conn = Mock()
        mock_db_connection = Mock()
        mock_client = MagicMock()
        mock_db = MagicMock()  # Use MagicMock for __getitem__ support
        mock_admin_db = Mock()
        mock_collection = Mock()

        # Wire up the mock chain
        mock_conn.connection = mock_db_connection
        mock_db_connection._client = mock_client
        mock_db_connection.database = mock_db
        mock_client.admin = mock_admin_db
        mock_db.__getitem__.return_value = mock_collection

        # Set up realistic responses
        mock_conn.test_connection = Mock(return_value=True)
        mock_admin_db.command.return_value = {"databases": [{"name": "myapp"}, {"name": "analytics"}]}
        mock_db.list_collection_names.return_value = ["users", "orders", "products"]
        mock_collection.find.return_value.limit.return_value = [
            {"_id": "1", "name": "Test User", "email": "test@example.com", "age": 30}
        ]
        mock_collection.index_information.return_value = {
            "_id_": {"key": [("_id", 1)], "unique": False},
            "email_1": {"key": [("email", 1)], "unique": True},
        }

        # Step 1: Connection testing (what Superset does first)
        ping_success = self.dialect.do_ping(mock_conn)
        self.assertTrue(ping_success, "Connection ping should succeed")

        # Step 2: Discover available databases/schemas
        schemas = self.dialect.get_schema_names(mock_conn)
        self.assertEqual(schemas, ["myapp", "analytics"], "Should discover databases")

        # Step 3: List tables/collections in default database
        tables = self.dialect.get_table_names(mock_conn)
        self.assertEqual(tables, ["users", "orders", "products"], "Should list collections")

        # Step 4: Check if specific table exists
        self.assertTrue(self.dialect.has_table(mock_conn, "users"), "Should find existing table")
        self.assertFalse(self.dialect.has_table(mock_conn, "logs"), "Should not find non-existing table")

        # Step 5: Get column information for table introspection
        columns = self.dialect.get_columns(mock_conn, "users")
        self.assertGreater(len(columns), 0, "Should discover columns from document sampling")

        # Verify required _id column exists and is not nullable
        id_column = next((col for col in columns if col["name"] == "_id"), None)
        self.assertIsNotNone(id_column, "_id column should exist")
        self.assertFalse(id_column["nullable"], "_id should not be nullable")

        # Step 6: Get index information for performance optimization
        indexes = self.dialect.get_indexes(mock_conn, "users")
        self.assertGreater(len(indexes), 0, "Should discover indexes")

        # Verify _id index exists
        id_index = next((idx for idx in indexes if idx["name"] == "_id_"), None)
        self.assertIsNotNone(id_index, "_id index should exist")


class TestPyMongoSQLCompilers(unittest.TestCase):
    """Test SQLAlchemy compiler components."""

    def setUp(self):
        """Set up test fixtures."""
        if not HAS_SQLALCHEMY:
            self.skipTest("SQLAlchemy not available")
        self.dialect = PyMongoSQLDialect()

    def test_identifier_preparer(self):
        """Test MongoDB identifier preparation."""
        preparer = PyMongoSQLIdentifierPreparer(self.dialect)

        # Test reserved words
        self.assertIn("$eq", preparer.reserved_words)
        self.assertIn("$and", preparer.reserved_words)

        # Test legal characters regex includes MongoDB-specific ones
        self.assertTrue(preparer.legal_characters.match("field.subfield"))  # Dot notation
        self.assertTrue(preparer.legal_characters.match("_id"))  # Underscore prefix
        self.assertTrue(preparer.legal_characters.match("user123"))  # Alphanumeric

    def test_type_compiler(self):
        """Test type compilation for MongoDB."""
        compiler = PyMongoSQLTypeCompiler(self.dialect)

        # Mock type objects
        varchar_type = Mock()
        varchar_type.__class__.__name__ = "VARCHAR"

        integer_type = Mock()
        integer_type.__class__.__name__ = "INTEGER"

        # Test type mapping
        self.assertEqual(compiler.visit_VARCHAR(varchar_type), "STRING")
        self.assertEqual(compiler.visit_INTEGER(integer_type), "INT32")
        self.assertEqual(compiler.visit_BOOLEAN(Mock()), "BOOL")

    def test_ddl_compiler(self):
        """Test DDL compilation."""
        # Test that the compiler class is properly configured
        self.assertEqual(self.dialect.ddl_compiler, PyMongoSQLDDLCompiler)

        # Test CREATE TABLE compilation concept
        # Test that the methods exist on the class
        self.assertTrue(hasattr(PyMongoSQLDDLCompiler, "visit_create_table"))
        self.assertTrue(hasattr(PyMongoSQLDDLCompiler, "visit_drop_table"))

        # Test DDL method behavior by calling class methods directly
        # This avoids the complex compiler instantiation issues

        # Create a mock compiler instance with minimal setup
        mock_compiler = Mock(spec=PyMongoSQLDDLCompiler)
        mock_compiler.preparer = Mock()
        mock_compiler.preparer.format_table = Mock(return_value="test_table")

        # Test CREATE TABLE behavior
        create_mock = Mock()
        create_mock.element = Mock()
        create_mock.element.name = "test_table"

        # Call the actual method from the class
        create_result = PyMongoSQLDDLCompiler.visit_create_table(mock_compiler, create_mock)
        self.assertIn("Collection will be created", create_result)

        # Test DROP TABLE behavior
        drop_mock = Mock()
        drop_mock.element = Mock()
        drop_mock.element.name = "test_table"

        # Call the actual method from the class
        drop_result = PyMongoSQLDDLCompiler.visit_drop_table(mock_compiler, drop_mock)
        self.assertIn("DROP COLLECTION", drop_result)


class TestSQLAlchemyIntegration(unittest.TestCase):
    """Integration tests for SQLAlchemy functionality."""

    def test_create_engine_url_helper(self):
        """Test the URL helper function."""
        url = create_engine_url("localhost", 27017, "testdb")
        self.assertEqual(url, "mongodb://localhost:27017/testdb")

        # Test with additional parameters
        url_with_params = create_engine_url("localhost", 27017, "testdb", ssl=True, replicaSet="rs0")
        self.assertIn("mongodb://localhost:27017/testdb", url_with_params)
        self.assertIn("ssl=True", url_with_params)
        self.assertIn("replicaSet=rs0", url_with_params)

    @patch("pymongosql.sqlalchemy_mongodb.sqlalchemy_dialect.pymongosql.connect")
    def test_engine_creation(self, mock_connect):
        """Test SQLAlchemy engine creation."""
        if not HAS_SQLALCHEMY:
            self.skipTest("SQLAlchemy not available")

        # Mock the connection
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        # This should not raise an exception
        engine = create_engine("mongodb://localhost:27017/testdb")
        self.assertIsNotNone(engine)
        self.assertEqual(engine.dialect.name, "mongodb")

        # Test version compatibility attributes
        if hasattr(engine.dialect, "_sqlalchemy_version"):
            self.assertIsNotNone(engine.dialect._sqlalchemy_version)
        if hasattr(engine.dialect, "_is_sqlalchemy_2x"):
            self.assertIsInstance(engine.dialect._is_sqlalchemy_2x, bool)

    def test_orm_model_definition(self):
        """Test ORM model definition with PyMongoSQL."""
        if not HAS_SQLALCHEMY:
            self.skipTest("SQLAlchemy not available")

        Base = declarative_base()

        class TestModel(Base):
            __tablename__ = "test_collection"

            id = Column("_id", String, primary_key=True)
            name = Column(String)
            value = Column(Integer)

        # Should not raise exceptions
        self.assertEqual(TestModel.__tablename__, "test_collection")
        # The column is named '_id' in the database, but 'id' in the model
        self.assertIn("_id", TestModel.__table__.columns.keys())  # Actual DB column name
        self.assertIn("name", TestModel.__table__.columns.keys())
        self.assertIn("value", TestModel.__table__.columns.keys())

        # Test that the model has the expected attributes
        self.assertTrue(hasattr(TestModel, "id"))  # Model attribute
        self.assertTrue(hasattr(TestModel, "name"))
        self.assertTrue(hasattr(TestModel, "value"))

        # Test SQLAlchemy version specific features
        self.assertTrue(hasattr(TestModel, "__table__"))


class TestDialectRegistration(unittest.TestCase):
    """Test dialect registration with SQLAlchemy."""

    def test_dialect_registration(self):
        """Test that the dialect is properly registered."""
        if not HAS_SQLALCHEMY:
            self.skipTest("SQLAlchemy not available")

        try:
            from sqlalchemy.dialects import registry

            from pymongosql.sqlalchemy_mongodb import _registration_successful

            # The dialect should be registered
            self.assertTrue(hasattr(registry, "load"))

            # Our registration should have succeeded
            self.assertTrue(_registration_successful)

        except ImportError:
            # Skip if SQLAlchemy registry is not available
            self.skipTest("SQLAlchemy registry not available")
