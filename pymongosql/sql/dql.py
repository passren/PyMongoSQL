# -*- coding: utf-8 -*-
import logging
from .base import Executor, QueryExecutor

_logger = logging.getLogger(__name__)  # type: ignore


class MongoQuery:
    def __init__(
        self, collection: str = None, filter: dict = None, projection: dict = None
    ):
        self._collection = collection
        self._filter = filter
        self._projection = projection
        self.sort = []
        self.limit = None

    @property
    def collection(self) -> str:
        return self._collection

    @collection.setter
    def collection(self, value: str):
        self._collection = value

    @property
    def filter(self) -> dict:
        return self._filter

    @filter.setter
    def filter(self, value: dict):
        self._filter = value

    @property
    def projection(self) -> dict:
        return self._projection

    @projection.setter
    def projection(self, value: dict):
        self._projection = value

    @property
    def sort(self) -> list:
        return self._sort

    @sort.setter
    def sort(self, value: list):
        self._sort = value

    @property
    def limit(self) -> int:
        return self._limit

    @limit.setter
    def limit(self, value: int):
        self._limit = value

    def transform(self) -> Executor:
        return QueryExecutor()
