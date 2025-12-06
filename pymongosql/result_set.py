# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

from .error import ProgrammingError
from .common import CursorIterator


if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore


class MongoResultSet(CursorIterator):
    def __init__(self, connection: "Connection", arraysize: int, **kwargs) -> None:
        super().__init__(arraysize=arraysize)
        ...

    @property
    def connection(self) -> "Connection":
        if self.is_closed:
            raise ProgrammingError("MongoResultSet is closed.")
        return cast("Connection", self._connection)

    @property
    def errors(self) -> List[Dict[str, str]]: ...

    @property
    def rowcount(self) -> int: ...

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]: ...

    def fetchone(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]: ...

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[Dict[Any, Optional[Any]]]: ...

    def fetchall(self) -> List[Dict[Any, Optional[Any]]]: ...

    @property
    def is_closed(self) -> bool:
        return self._connection is None

    def close(self) -> None: ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
