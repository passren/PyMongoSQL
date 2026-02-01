# -*- coding: utf-8 -*-
"""
WHERE Clause Functions - Convert string values to MongoDB types for filtering

These functions are specifically for WHERE clause expressions.
They convert string/value inputs to appropriate MongoDB types for correct filtering.

Differences from projection functions:
- Projection functions (SELECT clause): MongoDB type → Python type (for display)
- WHERE functions (WHERE clause): String value → MongoDB type (for filtering)
"""

import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from bson import Timestamp

_logger = logging.getLogger(__name__)


class WhereClauseFunction(ABC):
    """Base class for WHERE clause functions"""

    function_name: str = ""

    @abstractmethod
    def can_handle(self, text: str) -> bool:
        """Check if this function can handle the given expression"""
        pass

    @abstractmethod
    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract value and optional format parameter from function call"""
        pass

    @abstractmethod
    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert string value to MongoDB-compatible type for filtering"""
        pass


class WhereDateFunction(WhereClauseFunction):
    """DATE(value [, 'format']) - Convert to datetime for MongoDB BSON Date filtering

    In WHERE clauses, date() converts ISO date strings to datetime objects.
    MongoDB stores dates as BSON Date (64-bit milliseconds since epoch).
    PyMongo automatically converts Python datetime to BSON Date.

    Returns: datetime.datetime object (not date, since BSON requires datetime)
    """

    function_name = "DATE"

    def can_handle(self, text: str) -> bool:
        """Check if text is a DATE() function call"""
        return bool(re.match(r"^\s*DATE\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract value and optional format from DATE(value [, 'format'])"""
        match = re.match(
            r"^\s*DATE\s*\(\s*([^,)]+)(?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*$",
            text,
            re.IGNORECASE,
        )
        if match:
            value = match.group(1).strip()
            format_param = match.group(2) if match.group(2) else None
            return value, format_param
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to datetime object for MongoDB BSON Date filtering"""
        if value is None:
            return None

        if isinstance(value, datetime):
            # Ensure it has UTC timezone for consistency
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        if isinstance(value, str):
            # Try ISO format first
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, TypeError):
                pass

            # Try custom format if provided
            if format_param:
                try:
                    dt = datetime.strptime(value, format_param)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except (ValueError, TypeError):
                    pass

            # Try common formats - order matters for ambiguous dates
            for fmt in [
                "%Y-%m-%dT%H:%M:%SZ",  # ISO with time
                "%Y-%m-%d",  # ISO date
                "%d/%m/%Y",  # EU format DD/MM/YYYY
                "%d-%m-%Y",  # EU format DD-MM-YYYY
                "%d.%m.%Y",  # EU format DD.MM.YYYY
                "%m/%d/%Y",  # US format MM/DD/YYYY
                "%m-%d-%Y",  # US format MM-DD-YYYY
            ]:
                try:
                    dt = datetime.strptime(value, fmt)
                    # Add UTC timezone for consistency
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue

        return value


class WhereDatetimeFunction(WhereClauseFunction):
    """DATETIME(value [, 'format']) - Convert to datetime for MongoDB BSON DateTime filtering

    In WHERE clauses, datetime() converts ISO datetime strings to datetime objects.
    MongoDB stores datetimes as BSON DateTime (64-bit milliseconds since epoch).
    PyMongo automatically converts Python datetime to BSON DateTime.

    Returns: datetime.datetime object with timezone info
    """

    function_name = "DATETIME"

    def can_handle(self, text: str) -> bool:
        """Check if text is a DATETIME() function call"""
        return bool(re.match(r"^\s*DATETIME\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract value and optional format from DATETIME(value [, 'format'])"""
        match = re.match(
            r"^\s*DATETIME\s*\(\s*([^,)]+)(?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*$",
            text,
            re.IGNORECASE,
        )
        if match:
            value = match.group(1).strip()
            format_param = match.group(2) if match.group(2) else None
            return value, format_param
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to datetime object for MongoDB BSON DateTime filtering"""
        if value is None:
            return None

        if isinstance(value, datetime):
            # Ensure UTC timezone
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        if isinstance(value, str):
            # Try ISO format first (with or without timezone)
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, TypeError):
                pass

            # Try custom format if provided
            if format_param:
                try:
                    dt = datetime.strptime(value, format_param)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except (ValueError, TypeError):
                    pass

            # Try common datetime formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%SZ",  # ISO datetime with Z
                "%Y-%m-%d %H:%M:%S",  # Space-separated datetime
                "%Y-%m-%d",  # Date only
            ]:
                try:
                    dt = datetime.strptime(value, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue

        return value


class WhereTimestampFunction(WhereClauseFunction):
    """TIMESTAMP(value [, 'format']) - Convert to BSON Timestamp for filtering

    In WHERE clauses, timestamp() converts ISO datetime strings to BSON Timestamp.
    BSON Timestamp is: 4-byte seconds since epoch + 4-byte counter (internal MongoDB use).

    Returns: bson.Timestamp object
    """

    function_name = "TIMESTAMP"

    def can_handle(self, text: str) -> bool:
        """Check if text is a TIMESTAMP() function call"""
        return bool(re.match(r"^\s*TIMESTAMP\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract value and optional format from TIMESTAMP(value [, 'format'])"""
        match = re.match(
            r"^\s*TIMESTAMP\s*\(\s*([^,)]+)(?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*$",
            text,
            re.IGNORECASE,
        )
        if match:
            value = match.group(1).strip()
            format_param = match.group(2) if match.group(2) else None
            return value, format_param
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to BSON Timestamp for MongoDB filtering"""
        if value is None:
            return None

        if isinstance(value, Timestamp):
            return value

        if isinstance(value, (int, float)):
            # Unix timestamp
            return Timestamp(int(value), 0)

        if isinstance(value, datetime):
            # Convert datetime to unix timestamp
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            timestamp_int = int(value.timestamp())
            return Timestamp(timestamp_int, 0)

        if isinstance(value, str):
            # Try ISO format first
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                timestamp_int = int(dt.timestamp())
                return Timestamp(timestamp_int, 0)
            except (ValueError, TypeError):
                pass

            # Try date-only format (YYYY-MM-DD) - treat as UTC
            if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
                try:
                    dt = datetime.strptime(value, "%Y-%m-%d")
                    dt = dt.replace(tzinfo=timezone.utc)
                    timestamp_int = int(dt.timestamp())
                    return Timestamp(timestamp_int, 0)
                except ValueError:
                    pass

            # Try custom format if provided
            if format_param:
                try:
                    dt = datetime.strptime(value, format_param)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    timestamp_int = int(dt.timestamp())
                    return Timestamp(timestamp_int, 0)
                except ValueError:
                    pass

            # Try common formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d-%m-%Y",
                "%m/%d/%Y",
            ]:
                try:
                    dt = datetime.strptime(value, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    timestamp_int = int(dt.timestamp())
                    return Timestamp(timestamp_int, 0)
                except ValueError:
                    continue

        return value


class WhereClauseFunctionRegistry:
    """Registry for WHERE clause functions

    Manages available WHERE clause functions and provides lookup.
    Only supports: DATE, DATETIME, TIMESTAMP
    """

    _instance = None
    _functions = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize registry with WHERE clause functions"""
        if self._functions is None:
            self._functions = [
                WhereDateFunction(),
                WhereDatetimeFunction(),
                WhereTimestampFunction(),
            ]

    def find_function(self, text: str) -> Optional[WhereClauseFunction]:
        """Find a WHERE clause function that can handle the given text

        Args:
            text: The function call text, e.g., "date('2025-01-15')"

        Returns:
            WhereClauseFunction instance or None if no function matches
        """
        for func in self._functions:
            if func.can_handle(text):
                return func
        return None

    @property
    def functions(self):
        """Get all registered WHERE clause functions"""
        return self._functions
