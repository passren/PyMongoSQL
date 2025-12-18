# -*- coding: utf-8 -*-
"""
SQLAlchemy dialect for PyMongoSQL.

This module provides a SQLAlchemy dialect that allows PyMongoSQL to work
seamlessly with SQLAlchemy's ORM and core query functionality.

Supports both SQLAlchemy 1.x and 2.x versions.
"""
from typing import Any, Dict, List, Optional, Tuple, Type

try:
    import sqlalchemy

    SQLALCHEMY_VERSION = tuple(map(int, sqlalchemy.__version__.split(".")[:2]))
    SQLALCHEMY_2X = SQLALCHEMY_VERSION >= (2, 0)
except ImportError:
    SQLALCHEMY_VERSION = (1, 4)  # Default fallback
    SQLALCHEMY_2X = False

from sqlalchemy import pool, types
from sqlalchemy.engine import default, url
from sqlalchemy.sql import compiler
from sqlalchemy.sql.sqltypes import NULLTYPE

# Version-specific imports
if SQLALCHEMY_2X:
    try:
        from sqlalchemy.engine.interfaces import Dialect
    except ImportError:
        # Fallback for different 2.x versions
        from sqlalchemy.engine.default import DefaultDialect as Dialect
else:
    from sqlalchemy.engine.interfaces import Dialect

import pymongosql


class PyMongoSQLIdentifierPreparer(compiler.IdentifierPreparer):
    """MongoDB-specific identifier preparer.

    MongoDB collection and field names have specific rules that differ
    from SQL databases.
    """

    reserved_words = set(
        [
            # MongoDB reserved words and operators
            "$eq",
            "$ne",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
            "$in",
            "$nin",
            "$and",
            "$or",
            "$not",
            "$nor",
            "$exists",
            "$type",
            "$mod",
            "$regex",
            "$text",
            "$where",
            "$all",
            "$elemMatch",
            "$size",
            "$bitsAllClear",
            "$bitsAllSet",
            "$bitsAnyClear",
            "$bitsAnySet",
        ]
    )

    def __init__(self, dialect: Dialect, **kwargs: Any) -> None:
        super().__init__(dialect, **kwargs)
        # MongoDB allows most characters in field names - use regex pattern
        import re

        self.legal_characters = re.compile(r"^[$a-zA-Z0-9_.]+$")


class PyMongoSQLCompiler(compiler.SQLCompiler):
    """MongoDB-specific SQL compiler.

    Handles SQL compilation specific to MongoDB's query patterns.
    """

    def visit_column(self, column, **kwargs):
        """Handle column references for MongoDB field names."""
        name = column.name
        # Handle MongoDB-specific field name patterns
        if name.startswith("_"):
            # MongoDB system fields like _id
            return self.preparer.quote(name)
        return super().visit_column(column, **kwargs)


class PyMongoSQLDDLCompiler(compiler.DDLCompiler):
    """MongoDB-specific DDL compiler.

    Handles Data Definition Language operations for MongoDB.
    """

    def visit_create_table(self, create, **kwargs):
        """Handle CREATE TABLE - MongoDB creates collections on first insert."""
        # MongoDB collections are created implicitly
        return "-- Collection will be created on first insert"

    def visit_drop_table(self, drop, **kwargs):
        """Handle DROP TABLE - translates to MongoDB collection drop."""
        table = drop.element
        return f"-- DROP COLLECTION {self.preparer.format_table(table)}"


class PyMongoSQLTypeCompiler(compiler.GenericTypeCompiler):
    """MongoDB-specific type compiler.

    Handles type mapping between SQL types and MongoDB BSON types.
    """

    def visit_VARCHAR(self, type_, **kwargs):
        return "STRING"

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    def visit_TEXT(self, type_, **kwargs):
        return "STRING"

    def visit_INTEGER(self, type_, **kwargs):
        return "INT32"

    def visit_BIGINT(self, type_, **kwargs):
        return "INT64"

    def visit_FLOAT(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "DECIMAL128"

    def visit_DECIMAL(self, type_, **kwargs):
        return "DECIMAL128"

    def visit_DATETIME(self, type_, **kwargs):
        return "DATE"

    def visit_DATE(self, type_, **kwargs):
        return "DATE"

    def visit_BOOLEAN(self, type_, **kwargs):
        return "BOOL"


class PyMongoSQLDialect(default.DefaultDialect):
    """SQLAlchemy dialect for PyMongoSQL.

    This dialect enables PyMongoSQL to work with SQLAlchemy by providing
    the necessary interface methods and compilation logic.

    Compatible with SQLAlchemy 1.4+ and 2.x versions.
    """

    name = "pymongosql"
    driver = "pymongosql"

    # Version compatibility
    _sqlalchemy_version = SQLALCHEMY_VERSION
    _is_sqlalchemy_2x = SQLALCHEMY_2X

    # DB API 2.0 compliance
    supports_alter = False  # MongoDB doesn't support ALTER TABLE
    supports_comments = False  # No SQL comments in MongoDB
    supports_default_values = True
    supports_empty_inserts = True
    supports_multivalues_insert = True
    supports_native_decimal = True  # BSON Decimal128
    supports_native_boolean = True  # BSON Boolean
    supports_sequences = False  # No sequences in MongoDB
    supports_native_enum = False  # No native enums

    # MongoDB-specific features
    supports_statement_cache = True
    supports_server_side_cursors = True

    # Connection characteristics
    poolclass = pool.StaticPool

    # Compilation
    statement_compiler = PyMongoSQLCompiler
    ddl_compiler = PyMongoSQLDDLCompiler
    type_compiler = PyMongoSQLTypeCompiler
    preparer = PyMongoSQLIdentifierPreparer

    # Default parameter style
    paramstyle = "qmark"  # Matches PyMongoSQL's paramstyle

    @classmethod
    def dbapi(cls):
        """Return the PyMongoSQL DBAPI module (SQLAlchemy 1.x compatibility)."""
        return pymongosql

    @classmethod
    def import_dbapi(cls):
        """Return the PyMongoSQL DBAPI module (SQLAlchemy 2.x)."""
        return pymongosql

    def _get_dbapi_module(self):
        """Internal method to get DBAPI module for instance access."""
        return pymongosql

    def __getattribute__(self, name):
        """Override getattribute to handle DBAPI access properly."""
        if name == "dbapi":
            # Always return the module directly for DBAPI access
            return pymongosql
        return super().__getattribute__(name)

    def create_connect_args(self, url: url.URL) -> Tuple[List[Any], Dict[str, Any]]:
        """Create connection arguments from SQLAlchemy URL.

        Supports standard MongoDB connection strings (mongodb://).
        Note: For mongodb+srv URLs, use them directly as connection strings
        rather than through SQLAlchemy create_engine due to SQLAlchemy parsing limitations.

        Args:
            url: SQLAlchemy URL object with MongoDB connection string

        Returns:
            Tuple of (args, kwargs) for PyMongoSQL connection
        """
        opts = {}

        # For MongoDB URLs, reconstruct the full URI to pass to PyMongoSQL
        # This ensures proper MongoDB connection string format
        uri_parts = []

        # Start with scheme (mongodb only - srv handled separately)
        uri_parts.append(f"{url.drivername}://")

        # Add credentials if present
        if url.username:
            if url.password:
                uri_parts.append(f"{url.username}:{url.password}@")
            else:
                uri_parts.append(f"{url.username}@")

        # Add host and port
        if url.host:
            uri_parts.append(url.host)
            if url.port:
                uri_parts.append(f":{url.port}")

        # Add database
        if url.database:
            uri_parts.append(f"/{url.database}")

        # Add query parameters
        if url.query:
            query_parts = []
            for key, value in url.query.items():
                query_parts.append(f"{key}={value}")
            if query_parts:
                uri_parts.append(f"?{'&'.join(query_parts)}")

        # Pass the full MongoDB URI to PyMongoSQL
        mongodb_uri = "".join(uri_parts)
        opts["host"] = mongodb_uri

        return [], opts

    def get_schema_names(self, connection, **kwargs):
        """Get list of databases (schemas in SQL terms)."""
        # In MongoDB, databases are like schemas
        cursor = connection.execute("SHOW DATABASES")
        return [row[0] for row in cursor.fetchall()]

    def has_table(self, connection, table_name: str, schema: Optional[str] = None, **kwargs) -> bool:
        """Check if a collection (table) exists."""
        try:
            if schema:
                sql = f"SHOW COLLECTIONS FROM {schema}"
            else:
                sql = "SHOW COLLECTIONS"
            cursor = connection.execute(sql)
            collections = [row[0] for row in cursor.fetchall()]
            return table_name in collections
        except Exception:
            return False

    def get_table_names(self, connection, schema: Optional[str] = None, **kwargs) -> List[str]:
        """Get list of collections (tables)."""
        try:
            if schema:
                sql = f"SHOW COLLECTIONS FROM {schema}"
            else:
                sql = "SHOW COLLECTIONS"
            cursor = connection.execute(sql)
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []

    def get_columns(self, connection, table_name: str, schema: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        """Get column information for a collection.

        MongoDB is schemaless, so this inspects documents to infer structure.
        """
        columns = []
        try:
            # Use DESCRIBE-like functionality if available
            if schema:
                sql = f"DESCRIBE {schema}.{table_name}"
            else:
                sql = f"DESCRIBE {table_name}"

            cursor = connection.execute(sql)
            for row in cursor.fetchall():
                # Assume row format: (name, type, nullable, default)
                col_info = {
                    "name": row[0],
                    "type": self._get_column_type(row[1] if len(row) > 1 else "object"),
                    "nullable": row[2] if len(row) > 2 else True,
                    "default": row[3] if len(row) > 3 else None,
                }
                columns.append(col_info)
        except Exception:
            # Fallback: provide minimal _id column
            columns = [
                {
                    "name": "_id",
                    "type": types.String(),
                    "nullable": False,
                    "default": None,
                }
            ]

        return columns

    def _get_column_type(self, mongo_type: str) -> Type[types.TypeEngine]:
        """Map MongoDB/BSON types to SQLAlchemy types."""
        type_map = {
            "objectId": types.String,
            "string": types.String,
            "int": types.Integer,
            "long": types.BigInteger,
            "double": types.Float,
            "decimal": types.DECIMAL,
            "bool": types.Boolean,
            "date": types.DateTime,
            "null": NULLTYPE,
            "array": types.JSON,
            "object": types.JSON,
            "binData": types.LargeBinary,
        }
        return type_map.get(mongo_type.lower(), types.String)

    def get_pk_constraint(self, connection, table_name: str, schema: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Get primary key constraint info.

        MongoDB always has _id as the primary key.
        """
        return {"constrained_columns": ["_id"], "name": "pk_id"}

    def get_foreign_keys(
        self, connection, table_name: str, schema: Optional[str] = None, **kwargs
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints.

        MongoDB doesn't enforce foreign keys, return empty list.
        """
        return []

    def get_indexes(self, connection, table_name: str, schema: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        """Get index information for a collection."""
        indexes = []
        try:
            if schema:
                sql = f"SHOW INDEXES FROM {schema}.{table_name}"
            else:
                sql = f"SHOW INDEXES FROM {table_name}"

            cursor = connection.execute(sql)
            for row in cursor.fetchall():
                # Assume row format: (name, column_names, unique)
                index_info = {
                    "name": row[0],
                    "column_names": [row[1]] if isinstance(row[1], str) else row[1],
                    "unique": row[2] if len(row) > 2 else False,
                }
                indexes.append(index_info)
        except Exception:
            # Always include the default _id index
            indexes = [
                {
                    "name": "_id_",
                    "column_names": ["_id"],
                    "unique": True,
                }
            ]

        return indexes

    def do_rollback(self, dbapi_connection):
        """Rollback transaction.

        MongoDB has limited transaction support.
        """
        # PyMongoSQL should handle this
        if hasattr(dbapi_connection, "rollback"):
            dbapi_connection.rollback()

    def do_commit(self, dbapi_connection):
        """Commit transaction.

        MongoDB auto-commits most operations.
        """
        # PyMongoSQL should handle this
        if hasattr(dbapi_connection, "commit"):
            dbapi_connection.commit()


# Register the dialect with SQLAlchemy
# This allows using MongoDB connection strings directly
def register_dialect():
    """Register the PyMongoSQL dialect with SQLAlchemy.

    This function handles registration for both SQLAlchemy 1.x and 2.x.
    Registers support for standard MongoDB connection strings only.
    """
    try:
        from sqlalchemy.dialects import registry

        # Register for standard MongoDB URLs only
        registry.register("mongodb", "pymongosql.sqlalchemy_dialect", "PyMongoSQLDialect")
        # Note: mongodb+srv is handled by converting to mongodb in create_connect_args
        # SQLAlchemy doesn't support the + character in scheme names directly

        return True
    except ImportError:
        # Fallback for versions without registry
        return False
    except Exception:
        # Handle other registration errors gracefully
        return False


# Attempt registration on module import
_registration_successful = register_dialect()

# Version information
__sqlalchemy_version__ = SQLALCHEMY_VERSION
__supports_sqlalchemy_2x__ = SQLALCHEMY_2X
