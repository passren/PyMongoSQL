# -*- coding: utf-8 -*-
import logging
from typing import Optional, Type
from urllib.parse import quote_plus

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

from .error import NotSupportedError, DatabaseError, OperationalError
from .common import BaseCursor
from .cursor import Cursor

_logger = logging.getLogger(__name__)


class Connection:
    """MongoDB connection wrapper that provides SQL-like interface"""

    def __init__(
        self,
        host: str,
        port: int,
        database: str = None,
        username: str = None,
        password: str = None,
        auth_source: str = None,
        ssl: bool = None,
        ssl_cert_reqs: str = None,
        connection_timeout: int = None,
        server_selection_timeout: int = None,
        **kwargs,
    ) -> None:
        """Initialize MongoDB connection

        Args:
            host: MongoDB host (required)
            port: MongoDB port (required)
            database: Default database name (optional)
            username: Username for authentication (optional)
            password: Password for authentication (optional)
            auth_source: Authentication database (optional)
            ssl: Enable SSL (optional)
            ssl_cert_reqs: SSL certificate requirements (optional)
            connection_timeout: Connection timeout in ms (optional)
            server_selection_timeout: Server selection timeout in ms (optional)
            **kwargs: Additional PyMongo connection parameters
        """
        self._host = host
        self._port = port
        self._database_name = database
        self._username = username
        self._password = password
        self._auth_source = auth_source
        self._ssl = ssl
        self._ssl_cert_reqs = ssl_cert_reqs
        self._connection_timeout = connection_timeout
        self._server_selection_timeout = server_selection_timeout

        self._autocommit = True
        self._in_transaction = False
        self._client: Optional[MongoClient] = None
        self._database: Optional[Database] = None
        self.cursor_pool = []
        self.cursor_class = Cursor
        self.cursor_kwargs = kwargs

        # Establish connection
        self._connect()

    def _connect(self) -> None:
        """Establish connection to MongoDB"""
        try:
            # Build connection string
            if self._username and self._password:
                auth_string = f"{quote_plus(self._username)}:{quote_plus(self._password)}@"
            else:
                auth_string = ""

            connection_string = f"mongodb://{auth_string}{self._host}:{self._port}/"

            # Connection options with defaults only when needed
            options = {}

            if self._connection_timeout is not None:
                options["connectTimeoutMS"] = self._connection_timeout
            if self._server_selection_timeout is not None:
                options["serverSelectionTimeoutMS"] = self._server_selection_timeout
            if self._ssl is not None:
                options["ssl"] = self._ssl
            if self._ssl_cert_reqs is not None:
                options["ssl_cert_reqs"] = self._ssl_cert_reqs

            if self._username and self._auth_source:
                options["authSource"] = self._auth_source

            # Create client
            self._client = MongoClient(connection_string, **options)

            # Test connection
            self._client.admin.command("ping")

            # Set database if specified
            if self._database_name:
                self._database = self._client[self._database_name]

            _logger.info(f"Successfully connected to MongoDB at {self._host}:{self._port}")

        except ConnectionFailure as e:
            _logger.error(f"Failed to connect to MongoDB: {e}")
            raise OperationalError(f"Could not connect to MongoDB: {e}")
        except Exception as e:
            _logger.error(f"Unexpected error during connection: {e}")
            raise DatabaseError(f"Database connection error: {e}")

    @property
    def client(self) -> MongoClient:
        """Get the PyMongo client"""
        if self._client is None:
            raise OperationalError("No active connection")
        return self._client

    @property
    def database(self) -> Database:
        """Get the current database"""
        if self._database is None:
            raise OperationalError("No database selected")
        return self._database

    def use_database(self, database_name: str) -> None:
        """Switch to a different database"""
        if self._client is None:
            raise OperationalError("No active connection")
        self._database_name = database_name
        self._database = self._client[database_name]
        _logger.info(f"Switched to database: {database_name}")

    def get_collection(self, collection_name: str) -> Collection:
        """Get a collection from the current database"""
        if self._database is None:
            raise OperationalError("No database selected")
        return self._database[collection_name]

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        try:
            if not self._autocommit and value:
                self._autocommit = True
                for cursor_ in self.cursor_pool:
                    cursor_.flush()
        finally:
            self._autocommit = value

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction

    @in_transaction.setter
    def in_transaction(self, value: bool) -> bool:
        self._in_transaction = False

    @property
    def host(self) -> str:
        """Get the hostname"""
        return self._host

    @property
    def port(self) -> int:
        """Get the port number"""
        return self._port

    @property
    def database_name(self) -> str:
        """Get the database name"""
        return self._database_name

    @property
    def username(self) -> str:
        """Get the username"""
        return self._username

    @property
    def password(self) -> str:
        """Get the password"""
        return self._password

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def is_connected(self) -> bool:
        """Check if connected to MongoDB"""
        return self._client is not None

    @property
    def database_instance(self):
        """Get the database instance"""
        return self._database

    def disconnect(self) -> None:
        """Disconnect from MongoDB (alias for close)"""
        self.close()

    def __str__(self) -> str:
        """String representation of the connection"""
        status = "connected" if self.is_connected else "disconnected"
        return f"Connection(host={self._host}, port={self._port}, database={self._database_name}, status={status})"

    def cursor(self, cursor: Optional[Type[BaseCursor]] = None, **kwargs) -> BaseCursor:
        kwargs.update(self.cursor_kwargs)
        if not cursor:
            cursor = self.cursor_class

        new_cursor = cursor(
            connection=self,
            **kwargs,
        )
        self.cursor_pool.append(new_cursor)
        return new_cursor

    def close(self) -> None:
        """Close the MongoDB connection"""
        try:
            # Close all cursors
            for cursor in self.cursor_pool:
                cursor.close()
            self.cursor_pool.clear()

            # Close client connection
            if self._client:
                self._client.close()
                self._client = None
                self._database = None

            _logger.info("MongoDB connection closed")
        except Exception as e:
            _logger.error(f"Error closing connection: {e}")

    def begin(self) -> None:
        self._autocommit = False
        self._in_transaction = True

    def commit(self) -> None:
        """Commit transaction (MongoDB doesn't support traditional transactions in the same way)"""
        self._in_transaction = False
        self._autocommit = True

    def rollback(self) -> None:
        raise NotSupportedError("MongoDB doesn't support rollback in the traditional SQL sense")

    def test_connection(self) -> bool:
        """Test if the connection is alive"""
        try:
            if self._client:
                self._client.admin.command("ping")
                return True
            return False
        except Exception as e:
            _logger.error(f"Connection test failed: {e}")
            return False
