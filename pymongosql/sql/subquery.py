# -*- coding: utf-8 -*-
"""Subquery resolution and normalization utilities

Encapsulates logic for detecting wrapped subqueries in FROM clauses, parsing
inner SQL into an ExecutionPlan, and applying remapping/merging to an
outer ParseResult.

This keeps FromHandler lightweight and avoids mixing parsing logic in the
main handler module.
"""
import logging
import re
from typing import Any, Optional, Tuple

_logger = logging.getLogger(__name__)


class SubqueryResolver:
    """Resolve and apply FROM subquery expressions.

    Public methods:
        is_wrapped_subquery_text(text) -> bool
        resolve_and_apply(table_text, parse_result) -> collection_name
    """

    @staticmethod
    def is_wrapped_subquery_text(text: str) -> bool:
        t = text.strip()
        return t.startswith("(") and "SELECT" in t.upper()

    def _strip_alias_and_repair(self, text: str) -> Tuple[str, Optional[str]]:
        text = text.strip()
        try:
            first = text.index("(")
            last = text.rfind(")")
            if first == -1 or last == -1 or last <= first:
                return text, None
            inner = text[first + 1 : last].strip()
            rest = text[last + 1 :].strip()
            if rest.upper().startswith("AS "):
                rest = rest[3:].strip()
            alias = rest if rest else None

            # Repair collapsed inner SQL spacing heuristically so it can be reparsed
            repaired = re.sub(
                r"(?i)(SELECT|FROM|WHERE|AS|AND|OR|GROUP|ORDER|LIMIT|OFFSET|IN|ON|JOIN|LEFT|RIGHT|INNER|OUTER|HAVING)",
                r" \1 ",
                inner,
            )
            repaired = repaired.replace(",", ", ")
            repaired = re.sub(r"\s+", " ", repaired).strip()

            return repaired, alias
        except ValueError:
            return text, None

    def _rewrite_projection(self, projection: dict, alias: str) -> dict:
        new = {}
        for k, v in projection.items():
            if isinstance(k, str) and k.startswith(alias + "."):
                new_key = k[len(alias) + 1 :]
            else:
                new_key = k
            new[new_key] = v
        return new

    def _rewrite_filter_keys(self, obj: Any, alias: str) -> Any:
        if isinstance(obj, dict):
            new = {}
            for k, v in obj.items():
                if isinstance(k, str) and k.startswith(f"{alias}."):
                    new_key = k[len(alias) + 1 :]
                else:
                    new_key = k

                if k in {"$and", "$or", "$nor"} and isinstance(v, list):
                    new[new_key] = [self._rewrite_filter_keys(i, alias) for i in v]
                else:
                    new[new_key] = self._rewrite_filter_keys(v, alias)
            return new
        elif isinstance(obj, list):
            return [self._rewrite_filter_keys(i, alias) for i in obj]
        else:
            return obj

    def resolve_and_apply(self, table_text: str, parse_result: Any) -> str:
        """If `table_text` is a wrapped subquery, parse it and apply to parse_result.

        Returns the collection name to use (alias or inner collection or raw text on failure).
        """
        if not self.is_wrapped_subquery_text(table_text):
            return table_text

        inner_sql, alias = self._strip_alias_and_repair(table_text)

        try:
            # Import here to avoid circular imports at module import time
            from .parser import SQLParser

            inner_parser = SQLParser(inner_sql)
            inner_plan = inner_parser.get_execution_plan()

            # Attach to outer parse result
            parse_result.subquery_plan = inner_plan
            parse_result.subquery_alias = alias

            # Rewrite outer projection and filters that use the alias prefix
            if alias:
                if hasattr(parse_result, "projection") and parse_result.projection:
                    parse_result.projection = self._rewrite_projection(parse_result.projection, alias)

                if hasattr(parse_result, "filter_conditions") and parse_result.filter_conditions:
                    parse_result.filter_conditions = self._rewrite_filter_keys(parse_result.filter_conditions, alias)

            # Merge inner filters into outer filters
            if inner_plan.filter_stage:
                if not getattr(parse_result, "filter_conditions", None):
                    parse_result.filter_conditions = inner_plan.filter_stage
                else:
                    parse_result.filter_conditions = {"$and": [inner_plan.filter_stage, parse_result.filter_conditions]}

            # Use inner collection as effective collection
            parse_result.collection = inner_plan.collection
            return alias or inner_plan.collection

        except Exception as e:
            _logger.warning(f"Failed to parse subquery in FROM: {e}. Falling back to raw table text")
            parse_result.collection = table_text
            return table_text
