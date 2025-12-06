# -*- coding: utf-8 -*-
import logging
from typing import Optional, Type

from .error import NotSupportedError
from .common import BaseCursor

_logger = logging.getLogger(__name__)


class Connection:

    def __init__(
        self,
        **kwargs,
    ) -> None: ...

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def cursor(self, cursor: Optional[Type[BaseCursor]] = None, **kwargs) -> BaseCursor:
        kwargs.update(self.cursor_kwargs)
        if not cursor:
            cursor = self.cursor_class

        return cursor(
            connection=self,
            **kwargs,
        )

    def close(self) -> None: ...

    def begin(self) -> None:
        self._autocommit = False
        self._in_transaction = True

    def commit(self) -> None: ...

    def rollback(self) -> None:
        raise NotSupportedError

    def test_connection(self) -> bool: ...
