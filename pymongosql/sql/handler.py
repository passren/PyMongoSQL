# -*- coding: utf-8 -*-
"""
Expression handlers for converting SQL expressions to MongoDB query format
"""
from typing import Any, Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass
import logging
import time
import re

from .partiql.PartiQLParser import PartiQLParser

_logger = logging.getLogger(__name__)

# Constants
COMPARISON_OPERATORS = [">=", "<=", "!=", "<>", "=", "<", ">"]
LOGICAL_OPERATORS = ["AND", "OR", "NOT"]
OPERATOR_MAP = {
    "=": "$eq",
    "!=": "$ne",
    "<>": "$ne",
    "<": "$lt",
    "<=": "$lte",
    ">": "$gt",
    ">=": "$gte",
    "LIKE": "$regex",
    "IN": "$in",
    "NOT IN": "$nin",
}


@dataclass
class ExpressionResult:
    """Result of expression parsing"""

    mongo_filter: Dict[str, Any]
    field_references: List[str]
    has_errors: bool = False
    error_message: Optional[str] = None


class ContextUtilsMixin:
    """Mixin providing common context utility methods"""

    @staticmethod
    def get_context_text(ctx: Any) -> str:
        """Safely extract text from context"""
        return ctx.getText() if hasattr(ctx, "getText") else str(ctx)

    @staticmethod
    def get_context_type_name(ctx: Any) -> str:
        """Get context type name safely"""
        return type(ctx).__name__

    @staticmethod
    def has_children(ctx: Any) -> bool:
        """Check if context has children"""
        return hasattr(ctx, "children") and bool(ctx.children)


class LoggingMixin:
    """Mixin providing structured logging functionality"""

    def _log_operation_start(self, operation: str, ctx: Any, operation_id: int):
        """Log operation start with context"""
        _logger.debug(
            f"Starting {operation}",
            extra={
                "context_type": ContextUtilsMixin.get_context_type_name(ctx),
                "context_text": ContextUtilsMixin.get_context_text(ctx)[:100],
                "operation": operation,
                "operation_id": operation_id,
            },
        )

    def _log_operation_success(
        self, operation: str, operation_id: int, processing_time: float, **extra_data
    ):
        """Log successful operation completion"""
        log_data = {
            "operation": operation,
            "processing_time_ms": processing_time,
            "operation_id": operation_id,
        }
        log_data.update(extra_data)
        _logger.debug(f"{operation.title()} completed successfully", extra=log_data)

    def _log_operation_error(
        self,
        operation: str,
        ctx: Any,
        operation_id: int,
        processing_time: float,
        error: Exception,
    ):
        """Log operation error with context"""
        _logger.error(
            f"Failed to handle {operation}",
            extra={
                "error": str(error),
                "error_type": type(error).__name__,
                "context_text": ContextUtilsMixin.get_context_text(ctx),
                "context_type": ContextUtilsMixin.get_context_type_name(ctx),
                "operation": operation,
                "processing_time_ms": processing_time,
                "operation_id": operation_id,
            },
            exc_info=True,
        )


class OperatorExtractorMixin:
    """Mixin for extracting operators from expressions"""

    def _find_operator_in_text(self, text: str, operators: List[str]) -> Optional[str]:
        """Find first matching operator in text (ordered by length)"""
        for op in operators:
            if op in text:
                return op
        return None

    def _split_by_operator(self, text: str, operator: str) -> List[str]:
        """Split text by operator, returning non-empty parts"""
        parts = text.split(operator, 1)  # Split only on first occurrence
        return [part.strip() for part in parts if part.strip()]

    def _parse_value(self, value_text: str) -> Any:
        """Parse string value to appropriate Python type"""
        value_text = value_text.strip()

        # Remove quotes from string values
        if (value_text.startswith("'") and value_text.endswith("'")) or (
            value_text.startswith('"') and value_text.endswith('"')
        ):
            return value_text[1:-1]

        # Try to parse as number
        try:
            return int(value_text) if "." not in value_text else float(value_text)
        except ValueError:
            pass

        # Handle boolean values
        if value_text.lower() in ["true", "false"]:
            return value_text.lower() == "true"

        # Handle NULL
        if value_text.upper() == "NULL":
            return None

        return value_text


class ExpressionHandler(ABC):
    """Base class for expression handlers"""

    @abstractmethod
    def can_handle(self, ctx: Any) -> bool:
        """Check if this handler can process the given context"""
        pass

    @abstractmethod
    def handle(self, ctx: Any) -> ExpressionResult:
        """Handle the expression and return MongoDB filter"""
        pass


class ComparisonExpressionHandler(
    ExpressionHandler, ContextUtilsMixin, LoggingMixin, OperatorExtractorMixin
):
    """Handles comparison expressions like field = value, field > value, etc."""

    def can_handle(self, ctx: Any) -> bool:
        """Check if context represents a comparison expression"""
        try:
            text = self.get_context_text(ctx)
            text_upper = text.upper()

            # Count comparison operators
            comparison_count = sum(1 for op in COMPARISON_OPERATORS if op in text)

            # If there are multiple comparisons and logical operators, it's a logical expression
            has_logical_ops = any(op in text_upper for op in LOGICAL_OPERATORS)
            if has_logical_ops and comparison_count > 1:
                return False  # This should be handled by LogicalExpressionHandler
        except Exception as e:
            _logger.debug(f"ComparisonHandler: Error checking logical context: {e}")

        # Check various PartiQL expression types that represent comparisons
        return (
            hasattr(ctx, "comparisonOperator")
            or self._is_comparison_context(ctx)
            or self._has_comparison_pattern(ctx)
        )

    def handle(self, ctx: Any) -> ExpressionResult:
        """Convert comparison expression to MongoDB filter"""
        start_time = time.time()
        operation_id = id(ctx)
        self._log_operation_start("comparison_parsing", ctx, operation_id)

        try:
            field_name = self._extract_field_name(ctx)
            operator = self._extract_operator(ctx)
            value = self._extract_value(ctx)

            mongo_filter = self._build_mongo_filter(field_name, operator, value)

            processing_time = (time.time() - start_time) * 1000
            self._log_operation_success(
                "comparison_parsing",
                operation_id,
                processing_time,
                field_name=field_name,
                operator=operator,
            )

            return ExpressionResult(
                mongo_filter=mongo_filter, field_references=[field_name]
            )

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            self._log_operation_error(
                "comparison_parsing", ctx, operation_id, processing_time, e
            )
            return ExpressionResult(
                mongo_filter={},
                field_references=[],
                has_errors=True,
                error_message=str(e),
            )

    def _build_mongo_filter(
        self, field_name: str, operator: str, value: Any
    ) -> Dict[str, Any]:
        """Build MongoDB filter from field, operator and value"""
        if operator == "=":
            return {field_name: value}

        mongo_op = OPERATOR_MAP.get(operator.upper())
        if mongo_op == "$regex" and isinstance(value, str):
            # Convert SQL LIKE pattern to regex
            regex_pattern = value.replace("%", ".*").replace("_", ".")
            return {field_name: {"$regex": regex_pattern, "$options": "i"}}
        elif mongo_op:
            return {field_name: {mongo_op: value}}
        else:
            # Fallback to equality
            _logger.warning(f"Unknown operator '{operator}', falling back to equality")
            return {field_name: value}

    def _is_comparison_context(self, ctx: Any) -> bool:
        """Check if context is a comparison based on structure"""
        context_name = self.get_context_type_name(ctx).lower()
        structure_indicators = ["comparison", "predicate", "condition"]

        return (
            any(indicator in context_name for indicator in structure_indicators)
            or (hasattr(ctx, "left") and hasattr(ctx, "right"))
            or self._contains_comparison_operators(ctx)
        )

    def _has_comparison_pattern(self, ctx: Any) -> bool:
        """Check if the expression text contains comparison patterns"""
        try:
            text = self.get_context_text(ctx)
            return any(op in text for op in COMPARISON_OPERATORS + ["LIKE", "IN"])
        except Exception as e:
            _logger.debug(f"ComparisonHandler: Error checking comparison pattern: {e}")
            return False

    def _contains_comparison_operators(self, ctx: Any) -> bool:
        """Check if context contains comparison operators"""
        if not self.has_children(ctx):
            return False

        try:
            for child in ctx.children:
                child_text = self.get_context_text(child)
                if child_text in COMPARISON_OPERATORS:
                    return True
            return False
        except Exception:
            return False

    def _extract_field_name(self, ctx: Any) -> str:
        """Extract field name from comparison expression"""
        try:
            text = self.get_context_text(ctx)

            # Try operator-based splitting first
            operator = self._find_operator_in_text(text, COMPARISON_OPERATORS)
            if operator:
                parts = self._split_by_operator(text, operator)
                if parts:
                    field_part = parts[0].strip("'\"")
                    return field_part

            # If we can't parse it, look for identifiers in children
            if self.has_children(ctx):
                for child in ctx.children:
                    child_text = self.get_context_text(child)
                    # Skip operators and quoted values
                    if (
                        child_text not in COMPARISON_OPERATORS
                        and not child_text.startswith(("'", '"'))
                    ):
                        return child_text

            return "unknown_field"
        except Exception as e:
            _logger.debug(f"Failed to extract field name: {e}")
            return "unknown_field"

    def _extract_operator(self, ctx: Any) -> str:
        """Extract comparison operator"""
        try:
            text = self.get_context_text(ctx)

            # Look for operators in the text
            operator = self._find_operator_in_text(text, COMPARISON_OPERATORS)
            if operator:
                return operator

            # Check children for operator nodes
            if self.has_children(ctx):
                for child in ctx.children:
                    child_text = self.get_context_text(child)
                    if child_text in COMPARISON_OPERATORS:
                        return child_text

            return "="  # Default to equality
        except Exception as e:
            _logger.debug(f"Failed to extract operator: {e}")
            return "="

    def _extract_value(self, ctx: Any) -> Any:
        """Extract value from comparison expression"""
        try:
            text = self.get_context_text(ctx)

            # Find operator and split
            operator = self._find_operator_in_text(text, COMPARISON_OPERATORS)
            if operator:
                parts = self._split_by_operator(text, operator)
                if len(parts) >= 2:
                    return self._parse_value(parts[1])

            return None
        except Exception as e:
            _logger.debug(f"Failed to extract value: {e}")
            return None


class LogicalExpressionHandler(
    ExpressionHandler, ContextUtilsMixin, LoggingMixin, OperatorExtractorMixin
):
    """Handles logical expressions like AND, OR, NOT"""

    def can_handle(self, ctx: Any) -> bool:
        """Check if context represents a logical expression"""
        return (
            hasattr(ctx, "logicalOperator")
            or self._is_logical_context(ctx)
            or self._has_logical_operators(ctx)
        )

    def _has_logical_operators(self, ctx: Any) -> bool:
        """Check if the expression text contains logical operators"""
        try:
            text = self.get_context_text(ctx)
            text_upper = text.upper()

            # Count comparison operators to see if this looks like a logical expression
            comparison_count = sum(1 for op in COMPARISON_OPERATORS if op in text)

            # If there are multiple comparison operations and logical operators, it's likely logical
            has_logical_ops = any(
                op in text_upper for op in LOGICAL_OPERATORS[:2]
            )  # AND, OR only

            return has_logical_ops and comparison_count > 1
        except Exception as e:
            _logger.debug(f"LogicalHandler: Error checking logical operators: {e}")
            return False

    def _is_logical_context(self, ctx: Any) -> bool:
        """Check if context is a logical expression based on structure"""
        try:
            context_name = self.get_context_type_name(ctx).lower()
            logical_indicators = ["logical", "and", "or"]
            return any(
                indicator in context_name for indicator in logical_indicators
            ) or self._has_logical_operators(ctx)
        except Exception:
            return False

    def handle(self, ctx: Any) -> ExpressionResult:
        """Convert logical expression to MongoDB filter"""
        start_time = time.time()
        operation_id = id(ctx)
        self._log_operation_start("logical_parsing", ctx, operation_id)

        try:
            operator = self._extract_logical_operator(ctx)
            operands = self._extract_operands(ctx)

            # Process each operand recursively
            processed_operands, all_field_refs = self._process_operands(operands)

            # Combine operands based on logical operator
            mongo_filter = self._combine_operands(operator, processed_operands)

            processing_time = (time.time() - start_time) * 1000
            self._log_operation_success(
                "logical_parsing",
                operation_id,
                processing_time,
                operator=operator,
                processed_count=len(processed_operands),
            )

            return ExpressionResult(
                mongo_filter=mongo_filter, field_references=all_field_refs
            )

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            self._log_operation_error(
                "logical_parsing", ctx, operation_id, processing_time, e
            )
            return ExpressionResult(
                mongo_filter={},
                field_references=[],
                has_errors=True,
                error_message=str(e),
            )

    def _process_operands(
        self, operands: List[Any]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Process operands and return processed filters and field references"""
        processed_operands = []
        all_field_refs = []

        for operand in operands:
            handler = ExpressionHandlerFactory.get_handler(operand)
            if handler:
                result = handler.handle(operand)
                if not result.has_errors:
                    processed_operands.append(result.mongo_filter)
                    all_field_refs.extend(result.field_references)
                else:
                    _logger.warning(
                        f"Operand processing failed: {result.error_message}"
                    )
            else:
                _logger.warning(
                    f"No handler found for operand: {self.get_context_text(operand)}"
                )

        return processed_operands, all_field_refs

    def _combine_operands(
        self, operator: str, operands: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Combine operands based on logical operator"""
        if not operands:
            return {}

        if len(operands) == 1:
            return operands[0]

        operator_upper = operator.upper()
        if operator_upper == "AND":
            return {"$and": operands}
        elif operator_upper == "OR":
            return {"$or": operands}
        elif operator_upper == "NOT":
            return {"$not": operands[0]}
        else:
            _logger.warning(
                f"Unknown logical operator '{operator}', using empty filter"
            )
            return {}

    def _extract_logical_operator(self, ctx: Any) -> str:
        """Extract logical operator (AND, OR, NOT)"""
        try:
            text = self.get_context_text(ctx).upper()

            for op in LOGICAL_OPERATORS:
                if op in text:
                    return op

            return "AND"  # Default
        except Exception as e:
            _logger.debug(f"Failed to extract logical operator: {e}")
            return "AND"

    def _extract_operands(self, ctx: Any) -> List[Any]:
        """Extract operands for logical expression"""
        try:
            text = self.get_context_text(ctx)
            text_upper = text.upper()

            # Simple text-based splitting for AND/OR (no spaces in PartiQL output)
            if "AND" in text_upper:
                return self._split_operands_by_operator(text, "AND")
            elif "OR" in text_upper:
                return self._split_operands_by_operator(text, "OR")

            # Single operand
            return [self._create_operand_context(text)]

        except Exception as e:
            _logger.debug(f"Failed to extract operands: {e}")
            return []

    def _split_operands_by_operator(self, text: str, operator: str) -> List[Any]:
        """Split text by logical operator, handling quotes"""
        # Use regular expression to split on operator that's not inside quotes
        pattern = f"{operator}(?=(?:[^']*'[^']*')*[^']*$)"
        parts = re.split(pattern, text, flags=re.IGNORECASE)

        operand_contexts = []
        for part in parts:
            part = part.strip()
            if part:
                operand_contexts.append(self._create_operand_context(part))

        return operand_contexts

    def _create_operand_context(self, text: str):
        """Create a context-like object for operand text"""

        class SimpleContext:
            def __init__(self, text_content):
                self._text = text_content

            def getText(self):
                return self._text

        return SimpleContext(text)


class FunctionExpressionHandler(ExpressionHandler, ContextUtilsMixin, LoggingMixin):
    """Handles function expressions like COUNT(), MAX(), etc."""

    FUNCTION_MAP = {
        "COUNT": "$sum",
        "MAX": "$max",
        "MIN": "$min",
        "AVG": "$avg",
        "SUM": "$sum",
    }

    def can_handle(self, ctx: Any) -> bool:
        """Check if context represents a function call"""
        return hasattr(ctx, "functionName") or self._is_function_context(ctx)

    def handle(self, ctx: Any) -> ExpressionResult:
        """Handle function expressions"""
        start_time = time.time()
        operation_id = id(ctx)
        self._log_operation_start("function_parsing", ctx, operation_id)

        try:
            function_name = self._extract_function_name(ctx)
            arguments = self._extract_function_arguments(ctx)

            # For now, just return a placeholder - this would need full implementation
            mongo_filter = {
                "$expr": {
                    self.FUNCTION_MAP.get(function_name.upper(), "$sum"): arguments
                }
            }

            processing_time = (time.time() - start_time) * 1000
            self._log_operation_success(
                "function_parsing",
                operation_id,
                processing_time,
                function_name=function_name,
            )

            return ExpressionResult(
                mongo_filter=mongo_filter,
                field_references=(
                    arguments if isinstance(arguments, list) else [arguments]
                ),
            )

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            self._log_operation_error(
                "function_parsing", ctx, operation_id, processing_time, e
            )
            return ExpressionResult(
                mongo_filter={},
                field_references=[],
                has_errors=True,
                error_message=str(e),
            )

    def _is_function_context(self, ctx: Any) -> bool:
        """Check if context is a function call"""
        # TODO: Implement proper function detection
        return False

    def _extract_function_name(self, ctx: Any) -> str:
        """Extract function name"""
        # TODO: Implement proper function name extraction
        return "COUNT"

    def _extract_function_arguments(self, ctx: Any) -> List[str]:
        """Extract function arguments"""
        # TODO: Implement proper argument extraction
        return []


class ExpressionHandlerFactory:
    """Factory for creating appropriate expression handlers"""

    _handlers = [
        LogicalExpressionHandler(),  # Check logical first (AND/OR)
        ComparisonExpressionHandler(),  # Then simple comparisons
        FunctionExpressionHandler(),
    ]

    @classmethod
    def get_handler(cls, ctx: Any) -> Optional[ExpressionHandler]:
        """Get appropriate handler for the given context"""
        for handler in cls._handlers:
            if handler.can_handle(ctx):
                return handler
        return None

    @classmethod
    def register_handler(cls, handler: ExpressionHandler) -> None:
        """Register a new expression handler"""
        cls._handlers.append(handler)


class EnhancedWhereHandler(ContextUtilsMixin):
    """Enhanced WHERE clause handler using expression handlers"""

    def handle(self, ctx: PartiQLParser.WhereClauseSelectContext) -> Dict[str, Any]:
        """Handle WHERE clause with proper expression parsing"""
        if not hasattr(ctx, "exprSelect") or not ctx.exprSelect():
            _logger.debug("No expression found in WHERE clause")
            return {}

        expression_ctx = ctx.exprSelect()
        handler = ExpressionHandlerFactory.get_handler(expression_ctx)

        if handler:
            _logger.debug(
                f"Using {type(handler).__name__} for WHERE clause",
                extra={"context_text": self.get_context_text(expression_ctx)[:100]},
            )
            result = handler.handle(expression_ctx)
            if result.has_errors:
                _logger.warning(
                    "Expression parsing error, falling back to text search",
                    extra={"error": result.error_message},
                )
                # Fallback to text-based filter
                return {"$text": {"$search": self.get_context_text(expression_ctx)}}
            return result.mongo_filter
        else:
            # Fallback to simple text-based search
            _logger.debug(
                "No suitable expression handler found, using text search",
                extra={"context_text": self.get_context_text(expression_ctx)[:100]},
            )
            return {"$text": {"$search": self.get_context_text(expression_ctx)}}
