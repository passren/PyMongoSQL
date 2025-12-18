# -*- coding: utf-8 -*-
"""
SQLAlchemy MongoDB dialect and integration for PyMongoSQL.

This package provides SQLAlchemy integration including:
- MongoDB-specific dialect
- Version compatibility utilities
- Engine creation helpers
- MongoDB URI handling
"""

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


def register_dialect():
    """Register the PyMongoSQL dialect with SQLAlchemy.

    This function handles registration for both SQLAlchemy 1.x and 2.x.
    Registers support for standard MongoDB connection strings only.
    """
    try:
        from sqlalchemy.dialects import registry

        # Register for standard MongoDB URLs
        registry.register("mongodb", "pymongosql.sqlalchemy_mongodb.sqlalchemy_dialect", "PyMongoSQLDialect")

        # Try to register both SRV forms so SQLAlchemy can resolve SRV-style URLs
        # (either 'mongodb+srv' or the dotted 'mongodb.srv' plugin name).
        # Some SQLAlchemy versions accept '+' in scheme names; others import
        # the dotted plugin name. Attempt both registrations in one block.
        try:
            registry.register("mongodb+srv", "pymongosql.sqlalchemy_mongodb.sqlalchemy_dialect", "PyMongoSQLDialect")
            registry.register("mongodb.srv", "pymongosql.sqlalchemy_mongodb.sqlalchemy_dialect", "PyMongoSQLDialect")
        except Exception:
            # If registration fails we fall back to handling SRV URIs in
            # create_engine_from_mongodb_uri by converting 'mongodb+srv' to 'mongodb'.
            pass

        return True
    except ImportError:
        # Fallback for versions without registry
        return False
    except Exception:
        # Handle other registration errors gracefully
        return False


# Attempt registration on module import
_registration_successful = register_dialect()

# Export all SQLAlchemy-related functionality
__all__ = [
    "create_engine_url",
    "create_mongodb_url",
    "create_engine_from_mongodb_uri",
    "register_dialect",
    "__sqlalchemy_version__",
    "__supports_sqlalchemy__",
    "__supports_sqlalchemy_2x__",
    "_registration_successful",
]

# Note: PyMongoSQL now uses standard MongoDB connection strings directly
# No need for PyMongoSQL-specific URL format
