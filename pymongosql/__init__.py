# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, FrozenSet

from .error import *  # noqa

if TYPE_CHECKING:
    from .connection import Connection

__version__: str = "0.3.0"

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


# Register superset execution strategy for mongodb+superset:// connections
def _register_superset_executor() -> None:
    """Register SupersetExecution strategy for superset mode.

    This allows the executor and cursor to be unaware of superset -
    the execution strategy is automatically selected based on the connection mode.
    """
    try:
        from .executor import ExecutionPlanFactory
        from .superset_mongodb.executor import SupersetExecution

        ExecutionPlanFactory.register_strategy(SupersetExecution())
    except ImportError:
        # Superset module not available - skip registration
        pass


# Auto-register superset executor on module import
_register_superset_executor()

# SQLAlchemy integration (optional)
# For SQLAlchemy functionality, import from pymongosql.sqlalchemy_mongodb:
#   from pymongosql.sqlalchemy_mongodb import create_engine_url, create_engine_from_mongodb_uri
try:
    from .sqlalchemy_mongodb import __sqlalchemy_version__, __supports_sqlalchemy_2x__, __supports_sqlalchemy__
except ImportError:
    # SQLAlchemy integration not available
    __sqlalchemy_version__ = None
    __supports_sqlalchemy__ = False
    __supports_sqlalchemy_2x__ = False
