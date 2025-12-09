# -*- coding: utf-8 -*-
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import logging

from .partiql.PartiQLLexer import PartiQLLexer
from .partiql.PartiQLParser import PartiQLParser
from .partiql.PartiQLParserVisitor import PartiQLParserVisitor
from .builder import QueryPlan
from ..error import SqlSyntaxError

_logger = logging.getLogger(__name__)


@dataclass
class ParseContext:
    """Context object to maintain parsing state"""

    collection: Optional[str] = None
    projection: Dict[str, Any] = field(default_factory=dict)
    filter_conditions: Dict[str, Any] = field(default_factory=dict)
    sort_fields: List[Dict[str, int]] = field(default_factory=list)
    limit_value: Optional[int] = None
    offset_value: Optional[int] = None


class VisitorHandler(ABC):
    """Abstract base for visitor method handlers"""

    @abstractmethod
    def handle(self, ctx: Any, parse_context: ParseContext) -> Any:
        pass


class SelectHandler(VisitorHandler):
    """Handles SELECT statement parsing"""

    def handle(
        self, ctx: PartiQLParser.SelectItemsContext, parse_context: ParseContext
    ) -> Any:
        projection = {}

        if hasattr(ctx, "projectionItems") and ctx.projectionItems():
            for item in ctx.projectionItems().projectionItem():
                field_name = self._extract_field_name(item)
                projection[field_name] = 1

        parse_context.projection = projection
        return projection

    def _extract_field_name(self, item) -> str:
        """Extract field name from projection item"""
        if hasattr(item, "getText"):
            return item.getText().strip()
        return str(item)


class FromHandler(VisitorHandler):
    """Handles FROM clause parsing"""

    def handle(
        self, ctx: PartiQLParser.FromClauseContext, parse_context: ParseContext
    ) -> Any:
        if hasattr(ctx, "tableReference") and ctx.tableReference():
            collection_name = ctx.tableReference().getText()
            parse_context.collection = collection_name
            return collection_name
        return None


class WhereHandler(VisitorHandler):
    """Handles WHERE clause parsing"""

    def __init__(self):
        # Import here to avoid circular imports
        from .handler import EnhancedWhereHandler

        self._expression_handler = EnhancedWhereHandler()

    def handle(
        self, ctx: PartiQLParser.WhereClauseSelectContext, parse_context: ParseContext
    ) -> Any:
        if hasattr(ctx, "exprSelect") and ctx.exprSelect():
            try:
                # Use enhanced expression handler for better parsing
                filter_conditions = self._expression_handler.handle(ctx)
                parse_context.filter_conditions = filter_conditions
                return filter_conditions
            except Exception as e:
                _logger.warning(
                    f"Failed to parse WHERE expression, falling back to text search: {e}"
                )
                # Fallback to simple text search
                filter_text = ctx.exprSelect().getText()
                fallback_filter = {"$text": {"$search": filter_text}}
                parse_context.filter_conditions = fallback_filter
                return fallback_filter
        return {}


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
        self._parse_context = ParseContext()
        self._handlers = self._initialize_handlers()

    def _initialize_handlers(self) -> Dict[str, VisitorHandler]:
        """Initialize method handlers for better separation of concerns"""
        return {
            "select": SelectHandler(),
            "from": FromHandler(),
            "where": WhereHandler(),
        }

    @property
    def parse_context(self) -> ParseContext:
        """Get the current parse context"""
        return self._parse_context

    def parse_to_query_plan(self) -> QueryPlan:
        """Convert the parse context to a QueryPlan"""
        return QueryPlan(
            collection=self._parse_context.collection,
            filter_stage=self._parse_context.filter_conditions,
            projection_stage=self._parse_context.projection,
            sort_stage=self._parse_context.sort_fields,
            limit_stage=self._parse_context.limit_value,
            skip_stage=self._parse_context.offset_value,
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
        self._parse_context.projection = {}
        return self.visitChildren(ctx)

    def visitSelectItems(self, ctx: PartiQLParser.SelectItemsContext) -> Any:
        """Handle specific field selection in SELECT clause"""
        _logger.debug("Processing SELECT items")
        try:
            handler = self._handlers["select"]
            result = handler.handle(ctx, self._parse_context)
            return result
        except Exception as e:
            _logger.warning(f"Error processing SELECT items: {e}")
            return self.visitChildren(ctx)

    def visitFromClause(self, ctx: PartiQLParser.FromClauseContext) -> Any:
        """Handle FROM clause to extract collection/table name"""
        _logger.debug("Processing FROM clause")
        try:
            handler = self._handlers["from"]
            result = handler.handle(ctx, self._parse_context)
            _logger.debug(f"Extracted collection: {result}")
            return result
        except Exception as e:
            _logger.warning(f"Error processing FROM clause: {e}")
            return self.visitChildren(ctx)

    def visitWhereClauseSelect(
        self, ctx: PartiQLParser.WhereClauseSelectContext
    ) -> Any:
        """Handle WHERE clause for filtering"""
        _logger.debug("Processing WHERE clause")
        try:
            handler = self._handlers["where"]
            result = handler.handle(ctx, self._parse_context)
            _logger.debug(f"Extracted filter conditions: {result}")
            return result
        except Exception as e:
            _logger.warning(f"Error processing WHERE clause: {e}")
            return self.visitChildren(ctx)
