# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, FrozenSet

from .error import *  # noqa

if TYPE_CHECKING:
    from .connection import Connection

__version__: str = "0.2.0"

# Globals https://www.python.org/dev/peps/pep-0249/#globals
apilevel: str = "2.0"
threadsafety: int = 3
paramstyle: str = "qmark"


class DBAPITypeObject(FrozenSet[str]):
    """Type Objects and Constructors

    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
    """

    def __eq__(self, other: object):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        else:
            return other in self

    def __ne__(self, other: object):
        if isinstance(other, frozenset):
            return frozenset.__ne__(self, other)
        else:
            return other not in self

    def __hash__(self):
        return frozenset.__hash__(self)


def connect(*args, **kwargs) -> "Connection":
    from .connection import Connection

    return Connection(*args, **kwargs)


# SQLAlchemy integration
try:
    # Import and register the dialect automatically
    from .sqlalchemy_compat import (
        get_sqlalchemy_version,
        is_sqlalchemy_2x,
    )

    # Make compatibility info easily accessible
    __sqlalchemy_version__ = get_sqlalchemy_version()
    __supports_sqlalchemy__ = __sqlalchemy_version__ is not None
    __supports_sqlalchemy_2x__ = is_sqlalchemy_2x()

except ImportError:
    # SQLAlchemy not available
    __sqlalchemy_version__ = None
    __supports_sqlalchemy__ = False
    __supports_sqlalchemy_2x__ = False


def create_engine_url(host: str = "localhost", port: int = 27017, database: str = "test", **kwargs) -> str:
    """Create a SQLAlchemy engine URL for PyMongoSQL.

    Args:
        host: MongoDB host
        port: MongoDB port
        database: Database name
        **kwargs: Additional connection parameters

    Returns:
        SQLAlchemy URL string (uses mongodb:// format)

    Example:
        >>> url = create_engine_url("localhost", 27017, "mydb")
        >>> engine = sqlalchemy.create_engine(url)
    """
    params = []
    for key, value in kwargs.items():
        params.append(f"{key}={value}")

    param_str = "&".join(params)
    if param_str:
        param_str = "?" + param_str

    return f"mongodb://{host}:{port}/{database}{param_str}"


def create_mongodb_url(mongodb_uri: str) -> str:
    """Convert a standard MongoDB URI to work with PyMongoSQL SQLAlchemy dialect.

    Args:
        mongodb_uri: Standard MongoDB connection string
                    (e.g., 'mongodb://localhost:27017/mydb' or 'mongodb+srv://...')

    Returns:
        SQLAlchemy-compatible URL for PyMongoSQL

    Example:
        >>> url = create_mongodb_url("mongodb://user:pass@localhost:27017/mydb")
        >>> engine = sqlalchemy.create_engine(url)
    """
    # Return the MongoDB URI as-is since the dialect now handles MongoDB URLs directly
    return mongodb_uri


def create_engine_from_mongodb_uri(mongodb_uri: str, **engine_kwargs):
    """Create a SQLAlchemy engine from any MongoDB connection string.

    This function handles both mongodb:// and mongodb+srv:// URIs properly.
    Use this instead of create_engine() directly for mongodb+srv URIs.

    Args:
        mongodb_uri: Standard MongoDB connection string
        **engine_kwargs: Additional arguments passed to create_engine

    Returns:
        SQLAlchemy Engine object

    Example:
        >>> # For SRV records (Atlas/Cloud)
        >>> engine = create_engine_from_mongodb_uri("mongodb+srv://user:pass@cluster.net/db")
        >>> # For standard MongoDB
        >>> engine = create_engine_from_mongodb_uri("mongodb://localhost:27017/mydb")
    """
    try:
        from sqlalchemy import create_engine

        if mongodb_uri.startswith("mongodb+srv://"):
            # For MongoDB+SRV, convert to standard mongodb:// for SQLAlchemy compatibility
            # SQLAlchemy doesn't handle the + character in scheme names well
            converted_uri = mongodb_uri.replace("mongodb+srv://", "mongodb://")

            # Create engine with converted URI
            engine = create_engine(converted_uri, **engine_kwargs)

            def custom_create_connect_args(url):
                # Use original SRV URI for actual MongoDB connection
                opts = {"host": mongodb_uri}
                return [], opts

            engine.dialect.create_connect_args = custom_create_connect_args
            return engine
        else:
            # Standard mongodb:// URLs work fine with SQLAlchemy
            return create_engine(mongodb_uri, **engine_kwargs)

    except ImportError:
        raise ImportError("SQLAlchemy is required for engine creation")


# Note: PyMongoSQL now uses standard MongoDB connection strings directly
# No need for PyMongoSQL-specific URL format
