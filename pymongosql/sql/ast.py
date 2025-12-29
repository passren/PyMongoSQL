# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, Union

from ..error import SqlSyntaxError
from .builder import BuilderFactory
from .delete_builder import DeleteExecutionPlan
from .delete_handler import DeleteParseResult
from .handler import BaseHandler, HandlerFactory
from .insert_builder import InsertExecutionPlan
from .insert_handler import InsertParseResult
from .partiql.PartiQLLexer import PartiQLLexer
from .partiql.PartiQLParser import PartiQLParser
from .partiql.PartiQLParserVisitor import PartiQLParserVisitor
from .query_builder import QueryExecutionPlan
from .query_handler import QueryParseResult
from .update_builder import UpdateExecutionPlan
from .update_handler import UpdateParseResult

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
        self._parse_result = QueryParseResult.for_visitor()
        self._insert_parse_result = InsertParseResult.for_visitor()
        self._delete_parse_result = DeleteParseResult.for_visitor()
        self._update_parse_result = UpdateParseResult.for_visitor()
        # Track current statement kind generically so UPDATE/DELETE can reuse this
        self._current_operation: str = "select"  # expected values: select | insert | update | delete
        self._handlers = self._initialize_handlers()

    def _initialize_handlers(self) -> Dict[str, BaseHandler]:
        """Initialize method handlers for better separation of concerns"""
        # Use the factory to get pre-configured handlers
        return {
            "select": HandlerFactory.get_visitor_handler("select"),
            "from": HandlerFactory.get_visitor_handler("from"),
            "where": HandlerFactory.get_visitor_handler("where"),
            "insert": HandlerFactory.get_visitor_handler("insert"),
            "update": HandlerFactory.get_visitor_handler("update"),
            "delete": HandlerFactory.get_visitor_handler("delete"),
        }

    @property
    def parse_result(self) -> QueryParseResult:
        """Get the current parse result"""
        return self._parse_result

    def parse_to_execution_plan(
        self,
    ) -> Union[QueryExecutionPlan, InsertExecutionPlan, DeleteExecutionPlan, UpdateExecutionPlan]:
        """Convert the parse result to an execution plan using BuilderFactory."""
        if self._current_operation == "insert":
            return self._build_insert_plan()
        elif self._current_operation == "delete":
            return self._build_delete_plan()
        elif self._current_operation == "update":
            return self._build_update_plan()

        return self._build_query_plan()

    def _build_query_plan(self) -> QueryExecutionPlan:
        """Build a query execution plan from SELECT parsing."""
        builder = BuilderFactory.create_query_builder().collection(self._parse_result.collection)

        builder.filter(self._parse_result.filter_conditions).project(self._parse_result.projection).column_aliases(
            self._parse_result.column_aliases
        ).sort(self._parse_result.sort_fields).limit(self._parse_result.limit_value).skip(
            self._parse_result.offset_value
        )

        return builder.build()

    def _build_insert_plan(self) -> InsertExecutionPlan:
        """Build an INSERT execution plan from INSERT parsing."""
        if self._insert_parse_result.has_errors:
            raise SqlSyntaxError(self._insert_parse_result.error_message or "INSERT parsing failed")

        builder = BuilderFactory.create_insert_builder().collection(self._insert_parse_result.collection)

        documents = self._insert_parse_result.insert_documents or []
        builder.insert_documents(documents)

        if self._insert_parse_result.parameter_style:
            builder.parameter_style(self._insert_parse_result.parameter_style)

        if self._insert_parse_result.parameter_count > 0:
            builder.parameter_count(self._insert_parse_result.parameter_count)

        return builder.build()

    def _build_delete_plan(self) -> DeleteExecutionPlan:
        """Build a DELETE execution plan from DELETE parsing."""
        _logger.debug(
            f"Building DELETE plan with collection: {self._delete_parse_result.collection}, "
            f"filters: {self._delete_parse_result.filter_conditions}"
        )
        builder = BuilderFactory.create_delete_builder().collection(self._delete_parse_result.collection)

        if self._delete_parse_result.filter_conditions:
            builder.filter_conditions(self._delete_parse_result.filter_conditions)

        return builder.build()

    def _build_update_plan(self) -> UpdateExecutionPlan:
        """Build an UPDATE execution plan from UPDATE parsing."""
        _logger.debug(
            f"Building UPDATE plan with collection: {self._update_parse_result.collection}, "
            f"update_fields: {self._update_parse_result.update_fields}, "
            f"filters: {self._update_parse_result.filter_conditions}"
        )
        builder = BuilderFactory.create_update_builder().collection(self._update_parse_result.collection)

        if self._update_parse_result.update_fields:
            builder.update_fields(self._update_parse_result.update_fields)

        if self._update_parse_result.filter_conditions:
            builder.filter_conditions(self._update_parse_result.filter_conditions)

        return builder.build()

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

    def visitInsertStatement(self, ctx: PartiQLParser.InsertStatementContext) -> Any:
        """Handle INSERT statements via the insert handler."""
        _logger.debug("Processing INSERT statement")
        self._current_operation = "insert"
        handler = self._handlers.get("insert")
        if handler:
            return handler.handle_visitor(ctx, self._insert_parse_result)
        return self.visitChildren(ctx)

    def visitInsertStatementLegacy(self, ctx: PartiQLParser.InsertStatementLegacyContext) -> Any:
        """Handle legacy INSERT statements."""
        _logger.debug("Processing INSERT legacy statement")
        self._current_operation = "insert"
        handler = self._handlers.get("insert")
        if handler:
            return handler.handle_visitor(ctx, self._insert_parse_result)
        return self.visitChildren(ctx)

    def visitFromClauseSimpleExplicit(self, ctx: PartiQLParser.FromClauseSimpleExplicitContext) -> Any:
        """Handle FROM clause (explicit form) in DELETE statements."""
        if self._current_operation == "delete":
            handler = self._handlers.get("delete")
            if handler:
                return handler.handle_from_clause_explicit(ctx, self._delete_parse_result)
        return self.visitChildren(ctx)

    def visitFromClauseSimpleImplicit(self, ctx: PartiQLParser.FromClauseSimpleImplicitContext) -> Any:
        """Handle FROM clause (implicit form) in DELETE statements."""
        if self._current_operation == "delete":
            handler = self._handlers.get("delete")
            if handler:
                return handler.handle_from_clause_implicit(ctx, self._delete_parse_result)
        return self.visitChildren(ctx)

    def visitWhereClause(self, ctx: PartiQLParser.WhereClauseContext) -> Any:
        """Handle WHERE clause (generic form used in DELETE, UPDATE)."""
        _logger.debug("Processing WHERE clause (generic)")
        try:
            # For DELETE, use the delete handler
            if self._current_operation == "delete":
                handler = self._handlers.get("delete")
                if handler:
                    return handler.handle_where_clause(ctx, self._delete_parse_result)
                return {}
            # For UPDATE, use the update handler
            elif self._current_operation == "update":
                handler = self._handlers.get("update")
                if handler:
                    return handler.handle_where_clause(ctx, self._update_parse_result)
                return {}
            else:
                # For other operations, use the where handler
                handler = self._handlers["where"]
                if handler:
                    result = handler.handle_visitor(ctx, self._parse_result)
                    _logger.debug(f"Extracted filter conditions: {result}")
                    return result
            return {}
        except Exception as e:
            _logger.warning(f"Error processing WHERE clause: {e}")
            return {}

    def visitDeleteCommand(self, ctx: PartiQLParser.DeleteCommandContext) -> Any:
        """Handle DELETE statements."""
        _logger.debug("Processing DELETE statement")
        self._current_operation = "delete"
        # Reset delete parse result for this statement
        self._delete_parse_result = DeleteParseResult.for_visitor()
        # Use delete handler if available
        handler = self._handlers.get("delete")
        if handler:
            handler.handle_visitor(ctx, self._delete_parse_result)
        # Visit children to process FROM and WHERE clauses
        return self.visitChildren(ctx)

    def visitOrderByClause(self, ctx: PartiQLParser.OrderByClauseContext) -> Any:
        """Handle ORDER BY clause for sorting"""
        _logger.debug("Processing ORDER BY clause")

        try:
            sort_specs = []
            if hasattr(ctx, "orderSortSpec") and ctx.orderSortSpec():
                for sort_spec in ctx.orderSortSpec():
                    field_name = sort_spec.expr().getText() if sort_spec.expr() else "_id"
                    # Check for ASC/DESC (default is ASC = 1)
                    direction = 1  # ASC
                    if hasattr(sort_spec, "DESC") and sort_spec.DESC():
                        direction = -1  # DESC
                    # Convert to the expected format: List[Dict[str, int]]
                    sort_specs.append({field_name: direction})

            self._parse_result.sort_fields = sort_specs
            _logger.debug(f"Extracted sort specifications: {sort_specs}")
            return self.visitChildren(ctx)
        except Exception as e:
            _logger.warning(f"Error processing ORDER BY clause: {e}")
            return self.visitChildren(ctx)

    def visitLimitClause(self, ctx: PartiQLParser.LimitClauseContext) -> Any:
        """Handle LIMIT clause for result limiting"""
        _logger.debug("Processing LIMIT clause")
        try:
            if hasattr(ctx, "exprSelect") and ctx.exprSelect():
                limit_text = ctx.exprSelect().getText()
                try:
                    limit_value = int(limit_text)
                    self._parse_result.limit_value = limit_value
                    _logger.debug(f"Extracted limit value: {limit_value}")
                except ValueError as e:
                    _logger.warning(f"Invalid LIMIT value '{limit_text}': {e}")
            return self.visitChildren(ctx)
        except Exception as e:
            _logger.warning(f"Error processing LIMIT clause: {e}")
            return self.visitChildren(ctx)

    def visitOffsetByClause(self, ctx: PartiQLParser.OffsetByClauseContext) -> Any:
        """Handle OFFSET clause for result skipping"""
        _logger.debug("Processing OFFSET clause")
        try:
            if hasattr(ctx, "exprSelect") and ctx.exprSelect():
                offset_text = ctx.exprSelect().getText()
                try:
                    offset_value = int(offset_text)
                    self._parse_result.offset_value = offset_value
                    _logger.debug(f"Extracted offset value: {offset_value}")
                except ValueError as e:
                    _logger.warning(f"Invalid OFFSET value '{offset_text}': {e}")
            return self.visitChildren(ctx)
        except Exception as e:
            _logger.warning(f"Error processing OFFSET clause: {e}")
            return self.visitChildren(ctx)

    def visitUpdateClause(self, ctx: PartiQLParser.UpdateClauseContext) -> Any:
        """Handle UPDATE clause to extract collection/table name."""
        _logger.debug("Processing UPDATE clause")
        self._current_operation = "update"
        # Reset update parse result for this statement
        self._update_parse_result = UpdateParseResult.for_visitor()

        handler = self._handlers.get("update")
        if handler:
            handler.handle_visitor(ctx, self._update_parse_result)

        # Visit children to process SET and WHERE clauses
        return self.visitChildren(ctx)

    def visitSetCommand(self, ctx: PartiQLParser.SetCommandContext) -> Any:
        """Handle SET command for UPDATE statements."""
        _logger.debug("Processing SET command")

        if self._current_operation == "update":
            handler = self._handlers.get("update")
            if handler:
                handler.handle_set_command(ctx, self._update_parse_result)
                return None

        return self.visitChildren(ctx)
