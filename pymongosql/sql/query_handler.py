# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .handler import BaseHandler, ContextUtilsMixin
from .partiql.PartiQLParser import PartiQLParser

_logger = logging.getLogger(__name__)


@dataclass
class QueryParseResult:
    """Result container for query (SELECT) expression parsing and visitor state management"""

    # Core parsing fields
    filter_conditions: Dict[str, Any] = field(default_factory=dict)  # Unified filter field for all MongoDB conditions
    has_errors: bool = False
    error_message: Optional[str] = None

    # Visitor parsing state fields
    collection: Optional[str] = None
    projection: Dict[str, Any] = field(default_factory=dict)
    column_aliases: Dict[str, str] = field(default_factory=dict)  # Maps field_name -> alias
    sort_fields: List[Dict[str, int]] = field(default_factory=list)
    limit_value: Optional[int] = None
    offset_value: Optional[int] = None

    # Subquery info (for wrapped subqueries, e.g., Superset outering)
    subquery_plan: Optional[Any] = None
    subquery_alias: Optional[str] = None

    # Factory methods for different use cases
    @classmethod
    def for_visitor(cls) -> "QueryParseResult":
        """Create QueryParseResult for visitor parsing"""
        return cls()

    def merge_expression(self, other: "QueryParseResult") -> "QueryParseResult":
        """Merge expression results from another QueryParseResult"""
        if other.has_errors:
            self.has_errors = True
            self.error_message = other.error_message

        # Merge filter conditions intelligently
        if other.filter_conditions:
            if not self.filter_conditions:
                self.filter_conditions = other.filter_conditions
            else:
                # If both have filters, combine them with $and
                self.filter_conditions = {"$and": [self.filter_conditions, other.filter_conditions]}

        return self

    # Backward compatibility properties
    @property
    def mongo_filter(self) -> Dict[str, Any]:
        """Backward compatibility property for mongo_filter"""
        return self.filter_conditions

    @mongo_filter.setter
    def mongo_filter(self, value: Dict[str, Any]):
        """Backward compatibility setter for mongo_filter"""
        self.filter_conditions = value


class EnhancedWhereHandler(ContextUtilsMixin):
    """Enhanced WHERE clause handler using expression handlers"""

    def handle(self, ctx: PartiQLParser.WhereClauseSelectContext) -> Dict[str, Any]:
        """Handle WHERE clause with proper expression parsing"""
        if not hasattr(ctx, "exprSelect") or not ctx.exprSelect():
            _logger.debug("No expression found in WHERE clause")
            return {}

        expression_ctx = ctx.exprSelect()
        # Local import to avoid circular dependency between query_handler and handler
        from .handler import HandlerFactory

        handler = HandlerFactory.get_expression_handler(expression_ctx)

        if handler:
            _logger.debug(
                f"Using {type(handler).__name__} for WHERE clause",
                extra={"context_text": self.get_context_text(expression_ctx)[:100]},
            )
            result = handler.handle_expression(expression_ctx)
            if result.has_errors:
                _logger.warning(
                    "Expression parsing error, falling back to text search",
                    extra={"error": result.error_message},
                )
                # Fallback to text-based filter
                return {"$text": {"$search": self.get_context_text(expression_ctx)}}
            return result.filter_conditions
        else:
            # Fallback to simple text-based search
            _logger.debug(
                "No suitable expression handler found, using text search",
                extra={"context_text": self.get_context_text(expression_ctx)[:100]},
            )
            return {"$text": {"$search": self.get_context_text(expression_ctx)}}


class SelectHandler(BaseHandler, ContextUtilsMixin):
    """Handles SELECT statement parsing"""

    def can_handle(self, ctx: Any) -> bool:
        """Check if this is a select context"""
        return hasattr(ctx, "projectionItems")

    def handle_visitor(self, ctx: PartiQLParser.SelectItemsContext, parse_result: "QueryParseResult") -> Any:
        projection = {}
        column_aliases = {}

        if hasattr(ctx, "projectionItems") and ctx.projectionItems():
            for item in ctx.projectionItems().projectionItem():
                field_name, alias = self._extract_field_and_alias(item)
                # Use MongoDB standard projection format: {field: 1} to include field
                projection[field_name] = 1
                # Store alias if present
                if alias:
                    column_aliases[field_name] = alias

        parse_result.projection = projection
        parse_result.column_aliases = column_aliases
        return projection

    def _extract_field_and_alias(self, item) -> Tuple[str, Optional[str]]:
        """Extract field name and alias from projection item context with nested field support"""
        if not hasattr(item, "children") or not item.children:
            return str(item), None

        # According to grammar: projectionItem : expr ( AS? symbolPrimitive )? ;
        # children[0] is always the expression
        # If there's an alias, children[1] might be AS and children[2] symbolPrimitive
        # OR children[1] might be just symbolPrimitive (without AS)

        field_name = item.children[0].getText()
        # Normalize bracket notation (jmspath) to Mongo dot notation
        field_name = self.normalize_field_path(field_name)

        alias = None

        if len(item.children) >= 2:
            # Check if we have an alias
            if len(item.children) == 3:
                # Pattern: expr AS symbolPrimitive
                if hasattr(item.children[1], "getText") and item.children[1].getText().upper() == "AS":
                    alias = item.children[2].getText()
            elif len(item.children) == 2:
                # Pattern: expr symbolPrimitive (without AS)
                alias = item.children[1].getText()

        return field_name, alias


class FromHandler(BaseHandler):
    """Handles FROM clause parsing"""

    def can_handle(self, ctx: Any) -> bool:
        """Check if this is a from context"""
        return hasattr(ctx, "tableReference")

    def handle_visitor(self, ctx: PartiQLParser.FromClauseContext, parse_result: "QueryParseResult") -> Any:
        if hasattr(ctx, "tableReference") and ctx.tableReference():
            table_text = ctx.tableReference().getText()
            collection_name = table_text
            parse_result.collection = collection_name
            return collection_name
        return None


class WhereHandler(BaseHandler):
    """Handles WHERE clause parsing"""

    def __init__(self):
        self._expression_handler = EnhancedWhereHandler()

    def can_handle(self, ctx: Any) -> bool:
        """Check if this is a where context"""
        return hasattr(ctx, "exprSelect")

    def handle_visitor(self, ctx: PartiQLParser.WhereClauseSelectContext, parse_result: "QueryParseResult") -> Any:
        if hasattr(ctx, "exprSelect") and ctx.exprSelect():
            try:
                # Use enhanced expression handler for better parsing
                filter_conditions = self._expression_handler.handle(ctx)
                parse_result.filter_conditions = filter_conditions
                return filter_conditions
            except Exception as e:
                _logger.warning(f"Failed to parse WHERE expression, falling back to text search: {e}")
                # Fallback to simple text search
                filter_text = ctx.exprSelect().getText()
                fallback_filter = {"$text": {"$search": filter_text}}
                parse_result.filter_conditions = fallback_filter
                return fallback_filter
        return {}
