# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict

from ..error import SqlSyntaxError
from .builder import QueryPlan
from .handler import BaseHandler, HandlerFactory, ParseResult
from .partiql.PartiQLLexer import PartiQLLexer
from .partiql.PartiQLParser import PartiQLParser
from .partiql.PartiQLParserVisitor import PartiQLParserVisitor

_logger = logging.getLogger(__name__)


class MongoSQLLexer(PartiQLLexer):
    """Extended lexer for MongoDB SQL parsing"""

    pass


class MongoSQLParser(PartiQLParser):
    """Extended parser for MongoDB SQL parsing"""

    pass


class MongoSQLParserVisitor(PartiQLParserVisitor):
    """Enhanced visitor with structured handling and better readability"""

    def __init__(self) -> None:
        super().__init__()
        self._parse_result = ParseResult.for_visitor()
        self._handlers = self._initialize_handlers()

    def _initialize_handlers(self) -> Dict[str, BaseHandler]:
        """Initialize method handlers for better separation of concerns"""
        # Use the factory to get pre-configured handlers
        return {
            "select": HandlerFactory.get_visitor_handler("select"),
            "from": HandlerFactory.get_visitor_handler("from"),
            "where": HandlerFactory.get_visitor_handler("where"),
        }

    @property
    def parse_result(self) -> ParseResult:
        """Get the current parse result"""
        return self._parse_result

    def parse_to_query_plan(self) -> QueryPlan:
        """Convert the parse result to a QueryPlan"""
        return QueryPlan(
            collection=self._parse_result.collection,
            filter_stage=self._parse_result.filter_conditions,
            projection_stage=self._parse_result.projection,
            sort_stage=self._parse_result.sort_fields,
            limit_stage=self._parse_result.limit_value,
            skip_stage=self._parse_result.offset_value,
        )

    def visitRoot(self, ctx: PartiQLParser.RootContext) -> Any:
        """Visit root node and process child nodes"""
        _logger.debug("Starting to parse SQL query")
        try:
            result = self.visitChildren(ctx)
            return result
        except Exception as e:
            _logger.error(f"Error parsing root context: {e}")
            raise SqlSyntaxError(f"Failed to parse SQL query: {e}") from e

    def visitSelectAll(self, ctx: PartiQLParser.SelectAllContext) -> Any:
        """Handle SELECT * statements"""
        _logger.debug("Processing SELECT ALL statement")
        # SELECT * means no projection filter (return all fields)
        self._parse_result.projection = {}
        return self.visitChildren(ctx)

    def visitSelectItems(self, ctx: PartiQLParser.SelectItemsContext) -> Any:
        """Handle specific field selection in SELECT clause"""
        _logger.debug("Processing SELECT items")
        try:
            handler = self._handlers["select"]
            if handler:
                result = handler.handle_visitor(ctx, self._parse_result)
                return result
            return self.visitChildren(ctx)
        except Exception as e:
            _logger.warning(f"Error processing SELECT items: {e}")
            return self.visitChildren(ctx)

    def visitFromClause(self, ctx: PartiQLParser.FromClauseContext) -> Any:
        """Handle FROM clause to extract collection/table name"""
        _logger.debug("Processing FROM clause")
        try:
            handler = self._handlers["from"]
            if handler:
                result = handler.handle_visitor(ctx, self._parse_result)
                _logger.debug(f"Extracted collection: {result}")
                return result
            return self.visitChildren(ctx)
        except Exception as e:
            _logger.warning(f"Error processing FROM clause: {e}")
            return self.visitChildren(ctx)

    def visitWhereClauseSelect(self, ctx: PartiQLParser.WhereClauseSelectContext) -> Any:
        """Handle WHERE clause for filtering"""
        _logger.debug("Processing WHERE clause")
        try:
            handler = self._handlers["where"]
            if handler:
                result = handler.handle_visitor(ctx, self._parse_result)
                _logger.debug(f"Extracted filter conditions: {result}")
                return result
            return self.visitChildren(ctx)
        except Exception as e:
            _logger.warning(f"Error processing WHERE clause: {e}")
            return self.visitChildren(ctx)
