# -*- coding: utf-8 -*-
from abc import abstractmethod
import logging

_logger = logging.getLogger(__name__)  # type: ignore


class Executor(object):
    def __init__(self, type: str, **kwargs) -> None:
        self._type: str = type
        self._kwargs = kwargs

    @property
    def type(self) -> str:
        return self._type


class QueryExecutor(Executor):
    def __init__(self, type: str = "QUERY", **kwargs) -> None:
        super().__init__(type, **kwargs)


class MongoSQL(object):

    @abstractmethod
    def transform(self) -> Executor:
        pass  # pragma: no cover
