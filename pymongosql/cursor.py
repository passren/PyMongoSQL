# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypeVar, Tuple

from .common import BaseCursor, CursorIterator

if TYPE_CHECKING:
    from .connection import Connection

_logger = logging.getLogger(__name__)  # type: ignore
_T = TypeVar("_T", bound="Cursor")


class Cursor(BaseCursor, CursorIterator):
    NO_RESULT_SET = "No result set."

    def __init__(self, connection: "Connection", **kwargs) -> None:
        super().__init__(
            connection=connection,
            **kwargs,
        )
        self._kwargs = kwargs

    @property
    def result_set(self) -> Optional[CursorIterator]:
        return self._result_set

    @result_set.setter
    def result_set(self, val) -> None:
        self._result_set = val

    @property
    def has_result_set(self) -> bool:
        return self._result_set is not None

    @property
    def result_set_class(self) -> Optional[CursorIterator]:
        return self._result_set_class

    @result_set_class.setter
    def result_set_class(self, val) -> None:
        self._result_set_class = val

    @property
    def rowcount(self) -> int:
        return self._result_set.rowcount if self._result_set else -1

    @property
    def rownumber(self) -> Optional[int]:
        return self._result_set.rownumber if self._result_set else None

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, str, None, None, None, None, None]]]:
        return self._result_set.description

    @property
    def errors(self) -> List[Dict[str, str]]:
        return self._result_set.errors

    def execute(
        self: _T, operation: str, parameters: Optional[List[Dict[str, Any]]] = None
    ) -> _T: ...

    def executemany(
        self,
        operation: str,
        seq_of_parameters: List[Optional[Dict[str, Any]]],
    ) -> None: ...

    def execute_transaction(self) -> None: ...

    def flush(self) -> None: ...

    def fetchone(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]: ...

    def fetchmany(self, size: int = None) -> Optional[Dict[Any, Optional[Any]]]: ...

    def fetchall(
        self,
    ) -> Optional[Dict[Any, Optional[Any]]]: ...

    def close(self) -> None: ...
