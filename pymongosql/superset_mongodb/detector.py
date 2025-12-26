# -*- coding: utf-8 -*-
"""
Subquery detection and execution context management for handling Superset-style queries.

This module provides utilities to detect and manage the execution context for SQL queries
that contain subqueries, enabling the use of SQLite3 as an intermediate database for
complex query operations that MongoDB cannot handle natively.
"""

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

    # Pattern to detect wrapped subqueries: SELECT ... FROM (SELECT ...) AS alias
    WRAPPED_SUBQUERY_PATTERN = re.compile(
        r"SELECT\s+.*?\s+FROM\s*\(\s*(SELECT\s+.*?)\s*\)\s+(?:AS\s+)?(\w+)",
        re.IGNORECASE | re.DOTALL,
    )

    # Pattern to detect simple SELECT start
    SELECT_PATTERN = re.compile(r"^\s*SELECT\s+", re.IGNORECASE)

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

        # Check for wrapped subquery pattern (most common Superset case)
        match = cls.WRAPPED_SUBQUERY_PATTERN.search(query)
        if match:
            subquery_text = match.group(1)
            subquery_alias = match.group(2)

            if subquery_alias is None or subquery_alias == "":
                subquery_alias = "subquery_result"

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

        Returns:
            Tuple of (outer_query, subquery_alias) or None
        """
        info = cls.detect(query)
        if not info.is_wrapped:
            return None

        # Replace subquery with temporary table reference
        outer = cls.WRAPPED_SUBQUERY_PATTERN.sub(
            f"SELECT * FROM {info.subquery_alias}",
            query,
        )

        return outer, info.subquery_alias

    @classmethod
    def is_simple_select(cls, query: str) -> bool:
        """Check if query is a simple SELECT without subqueries"""
        info = cls.detect(query)
        return not info.has_subquery and cls.SELECT_PATTERN.match(query)
