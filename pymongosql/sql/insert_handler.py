# -*- coding: utf-8 -*-
"""INSERT parsing primitives kept separate for maintainability."""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
    def for_insert_visitor(cls) -> "InsertParseResult":
        """Factory for a fresh insert parse result."""
        return cls()


class InsertHandler(BaseHandler):
    """Placeholder handler for INSERT statements (parsing to be implemented)."""

    def can_handle(self, ctx: Any) -> bool:
        return hasattr(ctx, "INSERT")

    def handle_visitor(self, ctx: Any, parse_result: InsertParseResult) -> InsertParseResult:
        """Populate insert parse result; full parsing to be implemented later."""
        _logger.debug("INSERT handling not implemented yet; returning placeholder result")
        parse_result.has_errors = True
        parse_result.error_message = "INSERT parsing not implemented yet"
        return parse_result
