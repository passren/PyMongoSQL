# -*- coding: utf-8 -*-
import ast
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .handler import BaseHandler

_logger = logging.getLogger(__name__)


@dataclass
class InsertParseResult:
    """Result container for INSERT statement visitor parsing."""

    collection: Optional[str] = None
    insert_columns: Optional[List[str]] = None
    insert_values: Optional[List[List[Any]]] = None
    insert_documents: Optional[List[Dict[str, Any]]] = None
    insert_type: Optional[str] = None  # e.g., "values" | "bag"
    parameter_style: Optional[str] = None  # e.g., "qmark"
    parameter_count: int = 0
    has_errors: bool = False
    error_message: Optional[str] = None

    @classmethod
    def for_visitor(cls) -> "InsertParseResult":
        """Factory for a fresh insert parse result."""
        return cls()


class InsertHandler(BaseHandler):
    """Visitor handler to convert INSERT parse trees into InsertParseResult."""

    def can_handle(self, ctx: Any) -> bool:
        return hasattr(ctx, "INSERT")

    def handle_visitor(self, ctx: Any, parse_result: InsertParseResult) -> InsertParseResult:
        try:
            collection = self._extract_collection(ctx)
            value_text = self._extract_value_text(ctx)

            documents = self._parse_value_expr(value_text)
            param_style, param_count = self._detect_parameter_style(documents)

            parse_result.collection = collection
            parse_result.insert_documents = documents
            parse_result.insert_type = "bag" if value_text.strip().startswith("<<") else "value"
            parse_result.parameter_style = param_style
            parse_result.parameter_count = param_count
            parse_result.has_errors = False
            parse_result.error_message = None
            return parse_result
        except Exception as exc:  # pragma: no cover - defensive logging
            _logger.error("Failed to handle INSERT", exc_info=True)
            parse_result.has_errors = True
            parse_result.error_message = str(exc)
            return parse_result

    def _extract_collection(self, ctx: Any) -> str:
        if hasattr(ctx, "symbolPrimitive") and ctx.symbolPrimitive():
            return ctx.symbolPrimitive().getText()
        if hasattr(ctx, "pathSimple") and ctx.pathSimple():  # legacy form
            return ctx.pathSimple().getText()
        raise ValueError("INSERT statement missing collection name")

    def _extract_value_text(self, ctx: Any) -> str:
        if hasattr(ctx, "value") and ctx.value:
            return ctx.value.getText()
        if hasattr(ctx, "value") and callable(ctx.value):  # legacy form pathSimple VALUE expr
            value_ctx = ctx.value()
            if value_ctx:
                return value_ctx.getText()
        raise ValueError("INSERT statement missing value expression")

    def _parse_value_expr(self, text: str) -> List[Dict[str, Any]]:
        cleaned = text.strip()
        cleaned = self._normalize_literals(cleaned)

        if cleaned.startswith("<<") and cleaned.endswith(">>"):
            literal_text = cleaned.replace("<<", "[").replace(">>", "]")
            return self._parse_literal_list(literal_text)

        if cleaned.startswith("{") and cleaned.endswith("}"):
            doc = self._parse_literal_dict(cleaned)
            return [doc]

        raise ValueError("Unsupported INSERT value expression")

    def _parse_literal_list(self, literal_text: str) -> List[Dict[str, Any]]:
        try:
            value = ast.literal_eval(literal_text)
        except Exception as exc:
            raise ValueError(f"Failed to parse INSERT bag literal: {exc}") from exc
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise ValueError("INSERT bag must contain objects")
        return value

    def _parse_literal_dict(self, literal_text: str) -> Dict[str, Any]:
        try:
            value = ast.literal_eval(literal_text)
        except Exception as exc:
            raise ValueError(f"Failed to parse INSERT object literal: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError("INSERT value expression must be an object")
        return value

    def _normalize_literals(self, text: str) -> str:
        # Replace PartiQL-style booleans/null with Python equivalents for literal_eval
        replacements = {
            r"\bnull\b": "None",
            r"\bNULL\b": "None",
            r"\btrue\b": "True",
            r"\bTRUE\b": "True",
            r"\bfalse\b": "False",
            r"\bFALSE\b": "False",
        }
        normalized = text
        for pattern, replacement in replacements.items():
            normalized = re.sub(pattern, replacement, normalized)
        return normalized

    def _detect_parameter_style(self, documents: List[Dict[str, Any]]) -> Tuple[Optional[str], int]:
        style = None
        count = 0

        def consider(value: Any):
            nonlocal style, count
            if value == "?":
                new_style = "qmark"
            elif isinstance(value, str) and value.startswith(":"):
                new_style = "named"
            else:
                return

            if style and style != new_style:
                raise ValueError("Mixed parameter styles are not supported")
            style = new_style
            count += 1

        for doc in documents:
            for val in doc.values():
                consider(val)

        return style, count
