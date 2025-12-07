# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta
from typing import Any
from antlr4 import CommonTokenStream, InputStream
from .ast import MongoSQLLexer, MongoSQLParser, MongoSQLParserVisitor
from .base import Command
from ..error import SqlSyntaxError

_logger = logging.getLogger(__name__)  # type: ignore


class SQLParser(metaclass=ABCMeta):
    def __init__(self, sql: str) -> None:
        self._sql:str = sql
        self._ast: Any = None
        self._command: Command = None
        
        # Preprocess the statement before parsing
        self.preprocess()

        # Generate the AST
        self._load_ast()

    @property
    def sql(self):
        return self._sql

    def _load_ast(self) -> None:
        try:
            self._lexer = MongoSQLLexer(InputStream(self._sql))
            self._parser = MongoSQLParser(CommonTokenStream(self._lexer))
            self._ast = self._parser.root()
        except Exception as e:
            _logger.error("Failed to generate AST for [%s]: %s", self._sql , e)
            raise SqlSyntaxError from e

    def preprocess(self) -> None:
        pass  # pragma: no cover

    def get_command(self) -> Command:
        if self._command is not None:
            return self._command
        
        visitor = MongoSQLParserVisitor()
        self._command = visitor.visit(self._ast)
        return self._command