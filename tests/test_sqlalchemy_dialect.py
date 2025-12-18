#!/usr/bin/env python3
"""
Tests for PyMongoSQL SQLAlchemy dialect.

This test suite validates the SQLAlchemy integration functionality.
"""
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
        self.assertEqual(self.dialect.name, "pymongosql")
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

    @patch("pymongosql.connect")
    def test_has_table(self, mock_connect):
        """Test table (collection) existence check."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("users",), ("products",), ("orders",)]
        mock_conn.execute.return_value = mock_cursor

        # Test existing table
        self.assertTrue(self.dialect.has_table(mock_conn, "users"))

        # Test non-existing table
        self.assertFalse(self.dialect.has_table(mock_conn, "nonexistent"))

    @patch("pymongosql.connect")
    def test_get_table_names(self, mock_connect):
        """Test getting collection names."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("users",), ("products",), ("orders",)]
        mock_conn.execute.return_value = mock_cursor

        tables = self.dialect.get_table_names(mock_conn)
        expected = ["users", "products", "orders"]
        self.assertEqual(tables, expected)

    @patch("pymongosql.connect")
    def test_get_columns(self, mock_connect):
        """Test getting column information."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ("_id", "objectId", False, None),
            ("name", "string", True, None),
            ("age", "int", True, None),
            ("email", "string", False, None),
        ]
        mock_conn.execute.return_value = mock_cursor

        columns = self.dialect.get_columns(mock_conn, "users")

        self.assertEqual(len(columns), 4)
        self.assertEqual(columns[0]["name"], "_id")
        self.assertFalse(columns[0]["nullable"])
        self.assertEqual(columns[1]["name"], "name")
        self.assertTrue(columns[1]["nullable"])

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

    @patch("pymongosql.connect")
    def test_get_indexes(self, mock_connect):
        """Test getting index information."""
        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ("_id_", "_id", True),
            ("email_1", "email", True),
            ("name_1", "name", False),
        ]
        mock_conn.execute.return_value = mock_cursor

        indexes = self.dialect.get_indexes(mock_conn, "users")

        self.assertEqual(len(indexes), 3)
        self.assertEqual(indexes[0]["name"], "_id_")
        self.assertTrue(indexes[0]["unique"])
        self.assertEqual(indexes[1]["name"], "email_1")
        self.assertTrue(indexes[1]["unique"])


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
        self.assertEqual(engine.dialect.name, "pymongosql")

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
