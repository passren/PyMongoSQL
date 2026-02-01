# -*- coding: utf-8 -*-
import re
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class QueryInfo:
    """Information about a detected subquery"""

    has_subquery: bool = False
    is_wrapped: bool = False  # True if query is wrapped like SELECT * FROM (...) AS alias
    subquery_text: Optional[str] = None
    outer_query_text: Optional[str] = None
    subquery_alias: Optional[str] = None
    query_depth: int = 0  # Nesting depth


class SubqueryDetector:
    """Detects and analyzes SQL subqueries in query strings"""

    # Pattern to detect simple SELECT start
    SELECT_PATTERN = re.compile(r"^\s*SELECT\s+", re.IGNORECASE)

    @classmethod
    def _extract_balanced_subquery(cls, query: str) -> Optional[Tuple[str, str]]:
        """
        Extract subquery with balanced parentheses.

        Returns:
            Tuple of (subquery_text, alias) or None if not found
        """
        # Find FROM ( pattern
        from_match = re.search(r"FROM\s*\(\s*", query, re.IGNORECASE)
        if not from_match:
            return None

        start_pos = from_match.end()
        paren_count = 1
        pos = start_pos

        # Balance parentheses to find the matching closing paren
        while pos < len(query) and paren_count > 0:
            if query[pos] == "(":
                paren_count += 1
            elif query[pos] == ")":
                paren_count -= 1
            pos += 1

        if paren_count != 0:
            return None  # Unbalanced parentheses

        # Extract subquery text (between opening and closing parens)
        subquery_text = query[start_pos : pos - 1].strip()

        # Extract alias after the closing paren
        rest_of_query = query[pos:].strip()
        alias_match = re.match(r"(?:AS\s+)?(\w+)", rest_of_query, re.IGNORECASE)
        if alias_match:
            alias = alias_match.group(1)
        else:
            alias = "subquery_result"

        return subquery_text, alias

    @classmethod
    def detect(cls, query: str) -> QueryInfo:
        """
        Detect if a query contains subqueries.

        Args:
            query: SQL query string

        Returns:
            QueryInfo with detection results
        """
        query = query.strip()

        # Check for wrapped subquery pattern using balanced parentheses
        result = cls._extract_balanced_subquery(query)
        if result:
            subquery_text, subquery_alias = result

            return QueryInfo(
                has_subquery=True,
                is_wrapped=True,
                subquery_text=subquery_text,
                outer_query_text=query,
                subquery_alias=subquery_alias,
                query_depth=2,
            )

        # Check if query itself is a SELECT (no subquery)
        if cls.SELECT_PATTERN.match(query):
            return QueryInfo(
                has_subquery=False,
                is_wrapped=False,
                query_depth=1,
            )

        # Unknown pattern
        return QueryInfo(has_subquery=False)

    @classmethod
    def extract_subquery(cls, query: str) -> Optional[str]:
        """Extract the subquery text from a wrapped query"""
        info = cls.detect(query)
        return info.subquery_text if info.is_wrapped else None

    @classmethod
    def extract_outer_query(cls, query: str) -> Optional[Tuple[str, str]]:
        """
        Extract outer query with subquery placeholder.

        Preserves the complete outer query structure while replacing the subquery
        with a reference to the temporary table.

        Returns:
            Tuple of (outer_query, subquery_alias) or None if not a wrapped subquery
        """
        info = cls.detect(query)
        if not info.is_wrapped:
            return None

        # Use balanced parenthesis extraction to find subquery boundaries
        result = cls._extract_balanced_subquery(query)
        if not result:
            return None

        _, table_alias = result

        # Find the FROM ( pattern to locate where subquery starts
        from_match = re.search(r"FROM\s*\(", query, re.IGNORECASE)
        if not from_match:
            return None

        # Extract SELECT clause (everything before FROM ()
        select_clause = query[: from_match.start()].strip()

        # Find where the subquery ends (matching closing paren)
        start_pos = from_match.end()
        paren_count = 1
        pos = start_pos

        while pos < len(query) and paren_count > 0:
            if query[pos] == "(":
                paren_count += 1
            elif query[pos] == ")":
                paren_count -= 1
            pos += 1

        # Extract rest of query after the closing paren and alias
        rest_of_query = query[pos:].strip()
        # Remove the AS alias part if present
        rest_of_query = re.sub(r"^(?:AS\s+)?\w+\s*", "", rest_of_query, flags=re.IGNORECASE).strip()

        # Construct outer query with table alias replacing subquery
        if rest_of_query:
            outer = f"{select_clause} FROM {table_alias} {rest_of_query}"
        else:
            outer = f"{select_clause} FROM {table_alias}"

        return outer, table_alias

    @classmethod
    def is_simple_select(cls, query: str) -> bool:
        """Check if query is a simple SELECT without subqueries"""
        info = cls.detect(query)
        return not info.has_subquery and cls.SELECT_PATTERN.match(query)
