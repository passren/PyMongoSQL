# -*- coding: utf-8 -*-
import unittest
from typing import Callable
from unittest.mock import Mock, patch

import pytest

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

# Known test collections and views set up by run_test_server.py
EXPECTED_COLLECTIONS = {
    "users",
    "products",
    "categories",
    "orders",
    "analytics",
    "departments",
    "suppliers",
    "user-orders",
}
EXPECTED_VIEWS = {"active_users", "completed_orders", "in_stock_products", "orders_with_users"}

pytestmark = pytest.mark.skipif(not HAS_SQLALCHEMY, reason="SQLAlchemy not available")


class TestPyMongoSQLDialectUnit(unittest.TestCase):
    """Pure unit tests for dialect properties that don't need MongoDB."""

    def setUp(self):
        if not HAS_SQLALCHEMY:
            self.skipTest("SQLAlchemy not available")
        self.dialect = PyMongoSQLDialect()

    def test_dialect_name(self):
        """Test dialect name and driver."""
        self.assertEqual(self.dialect.name, "mongodb")
        self.assertEqual(self.dialect.driver, "pymongosql")

    def test_dbapi(self):
        """Test DBAPI module reference."""
        self.assertEqual(PyMongoSQLDialect.dbapi(), pymongosql)
        self.assertEqual(PyMongoSQLDialect.import_dbapi(), pymongosql)

        try:
            result = self.dialect.dbapi() if callable(self.dialect.dbapi) else self.dialect._get_dbapi_module()
            self.assertEqual(result, pymongosql)
        except Exception:
            self.assertEqual(PyMongoSQLDialect.dbapi(), pymongosql)

    def test_create_connect_args_basic(self):
        """Test basic connection argument creation."""
        test_url = url.make_url("mongodb://localhost:27017/testdb")
        args, kwargs = self.dialect.create_connect_args(test_url)

        self.assertEqual(args, [])
        self.assertIn("host", kwargs)
        self.assertEqual(kwargs["host"], "mongodb://localhost:27017/testdb")

    def test_create_connect_args_with_auth(self):
        """Test connection args with authentication."""
        test_url = url.make_url("mongodb://user:pass@localhost:27017/testdb")
        args, kwargs = self.dialect.create_connect_args(test_url)

        self.assertIn("host", kwargs)
        self.assertEqual(kwargs["host"], "mongodb://user:pass@localhost:27017/testdb")

    def test_create_connect_args_with_query_params(self):
        """Test connection args with query parameters."""
        test_url = url.make_url("mongodb://localhost/testdb?ssl=true&replicaSet=rs0")
        args, kwargs = self.dialect.create_connect_args(test_url)

        self.assertIn("host", kwargs)
        self.assertIn("ssl=true", kwargs["host"])
        self.assertIn("replicaSet=rs0", kwargs["host"])

    def test_supports_features(self):
        """Test dialect feature support flags."""
        self.assertFalse(self.dialect.supports_alter)
        self.assertFalse(self.dialect.supports_comments)
        self.assertFalse(self.dialect.supports_sequences)
        self.assertFalse(self.dialect.supports_native_enum)

        self.assertTrue(self.dialect.supports_default_values)
        self.assertTrue(self.dialect.supports_empty_inserts)
        self.assertTrue(self.dialect.supports_multivalues_insert)
        self.assertTrue(self.dialect.supports_native_decimal)
        self.assertTrue(self.dialect.supports_native_boolean)

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

    def test_infer_bson_type(self):
        """Test BSON type inference from Python values."""
        from datetime import datetime

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
                self.assertEqual(self.dialect._infer_bson_type(value), expected_type)


class TestPyMongoSQLDialectIntegration:
    """Integration tests for dialect introspection against real MongoDB."""

    def test_has_table_existing(self, sqlalchemy_engine):
        """Test that existing collections are found."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            assert dialect.has_table(conn, "users") is True
            assert dialect.has_table(conn, "products") is True
            assert dialect.has_table(conn, "orders") is True

    def test_has_table_nonexistent(self, sqlalchemy_engine):
        """Test that non-existing collections return False."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            assert dialect.has_table(conn, "nonexistent_xyz") is False

    def test_get_table_names(self, sqlalchemy_engine):
        """Test listing collections excludes views."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            tables = dialect.get_table_names(conn)

            assert isinstance(tables, list)
            # Should contain known test collections
            for collection in EXPECTED_COLLECTIONS:
                assert collection in tables, f"Expected collection '{collection}' not found in {tables}"
            # Should NOT contain views
            for view in EXPECTED_VIEWS:
                assert view not in tables, f"View '{view}' should not appear in table names"

    def test_get_view_names(self, sqlalchemy_engine):
        """Test listing views returns only views."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            views = dialect.get_view_names(conn)

            assert isinstance(views, list)
            # Should contain known test views
            for view in EXPECTED_VIEWS:
                assert view in views, f"Expected view '{view}' not found in {views}"
            # Should NOT contain regular collections
            for collection in EXPECTED_COLLECTIONS:
                assert collection not in views, f"Collection '{collection}' should not appear in view names"

    def test_get_columns_users(self, sqlalchemy_engine):
        """Test column inference from users collection."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            columns = dialect.get_columns(conn, "users")

            assert len(columns) > 0
            col_names = [c["name"] for c in columns]

            # Users collection should have these fields
            assert "_id" in col_names
            assert "name" in col_names
            assert "email" in col_names

            # _id should not be nullable
            id_col = next(c for c in columns if c["name"] == "_id")
            assert id_col["nullable"] is False

    def test_get_columns_products(self, sqlalchemy_engine):
        """Test column inference from products collection."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            columns = dialect.get_columns(conn, "products")

            col_names = [c["name"] for c in columns]
            assert "_id" in col_names
            assert "name" in col_names
            assert "price" in col_names
            assert "category" in col_names

    def test_get_columns_view(self, sqlalchemy_engine):
        """Test column inference works on views too."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            columns = dialect.get_columns(conn, "active_users")

            col_names = [c["name"] for c in columns]
            assert "_id" in col_names
            assert "name" in col_names
            assert "email" in col_names

    def test_get_indexes(self, sqlalchemy_engine):
        """Test getting index information from a real collection."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            indexes = dialect.get_indexes(conn, "users")

            assert len(indexes) >= 1
            # Every collection has at least the _id index
            id_index = next((idx for idx in indexes if idx["name"] == "_id_"), None)
            assert id_index is not None
            assert "_id" in id_index["column_names"]

    def test_get_schema_names(self, sqlalchemy_engine):
        """Test listing databases."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            schemas = dialect.get_schema_names(conn)

            assert isinstance(schemas, list)
            assert len(schemas) > 0
            # test_db should be among the databases
            assert "test_db" in schemas

    def test_do_ping(self, sqlalchemy_engine):
        """Test connection ping against real MongoDB."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            result = dialect.do_ping(conn.connection)
            assert result is True

    def test_superset_workflow(self, sqlalchemy_engine):
        """Test the complete introspection workflow Superset performs."""
        dialect = sqlalchemy_engine.dialect
        with sqlalchemy_engine.connect() as conn:
            # Step 1: Ping
            assert dialect.do_ping(conn.connection) is True

            # Step 2: Discover schemas
            schemas = dialect.get_schema_names(conn)
            assert len(schemas) > 0

            # Step 3: List tables (should not include views)
            tables = dialect.get_table_names(conn)
            assert "users" in tables
            for view in EXPECTED_VIEWS:
                assert view not in tables

            # Step 4: List views
            views = dialect.get_view_names(conn)
            for view in EXPECTED_VIEWS:
                assert view in views

            # Step 5: Check table existence
            assert dialect.has_table(conn, "users") is True
            assert dialect.has_table(conn, "nonexistent_xyz") is False

            # Step 6: Get columns
            columns = dialect.get_columns(conn, "users")
            assert len(columns) > 0
            id_col = next(c for c in columns if c["name"] == "_id")
            assert id_col["nullable"] is False

            # Step 7: Get indexes
            indexes = dialect.get_indexes(conn, "users")
            assert len(indexes) >= 1

            # Step 8: PK and FK constraints
            pk = dialect.get_pk_constraint(conn, "users")
            assert pk["constrained_columns"] == ["_id"]

            fks = dialect.get_foreign_keys(conn, "users")
            assert fks == []


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

    def test_srv_dialect_lookup(self):
        """Test that mongodb.srv resolves correctly (mongodb+srv:// URLs)."""
        if not HAS_SQLALCHEMY:
            self.skipTest("SQLAlchemy not available")

        from sqlalchemy.dialects import registry

        loaded = registry.load("mongodb.srv")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.name, "mongodb")
