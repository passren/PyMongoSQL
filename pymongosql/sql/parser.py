# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta
from typing import Any
from antlr4 import CommonTokenStream, InputStream
from .ast import MongoSQLLexer, MongoSQLParser, MongoSQLParserVisitor
from .base import Executor
from ..error import SqlSyntaxError

_logger = logging.getLogger(__name__)  # type: ignore


class SQLParser(metaclass=ABCMeta):
    def __init__(self, sql: str) -> None:
        self._sql: str = sql
        self._ast: Any = None

        # Preprocess the statement before parsing
        self.preprocess()

        # Generate the AST
        self._load_ast()

    @property
    def sql(self):
        return self._sql

    def _load_ast(self) -> None:
        try:
            lexer = MongoSQLLexer(InputStream(self._sql))
            parser = MongoSQLParser(CommonTokenStream(lexer))
            self._ast = parser.root()
        except Exception as e:
            _logger.error("Failed to generate AST for [%s]: %s", self._sql, e)
            raise SqlSyntaxError from e

    def preprocess(self) -> None:
        pass  # pragma: no cover

    def get_executor(self) -> Executor:
        visitor = MongoSQLParserVisitor()
        visitor.visit(self._ast)
        return visitor.mongo_sql.transform()
