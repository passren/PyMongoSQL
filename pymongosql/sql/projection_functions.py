# -*- coding: utf-8 -*-
import logging
import re
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, List, Optional, Tuple

from bson import Timestamp

_logger = logging.getLogger(__name__)


class ProjectionFunction(ABC):
    """Base class for projection functions"""

    function_name: str = ""

    @abstractmethod
    def can_handle(self, text: str) -> bool:
        """Check if this function can handle the given expression"""
        pass

    @abstractmethod
    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column name and optional format parameter from function call"""
        pass

    @abstractmethod
    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert value to target type"""
        pass

    def get_type_code(self) -> str:
        """Return type code for result description"""
        return "str"


class DateFunction(ProjectionFunction):
    """DATE(column [, 'format']) - Convert to date"""

    function_name = "DATE"

    def can_handle(self, text: str) -> bool:
        """Check if text is a DATE() function call"""
        return bool(re.match(r"^\s*DATE\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column and optional format from DATE(column [, 'format'])"""
        # Pattern: DATE(column_expr [, 'format'])
        match = re.match(r"^\s*DATE\s*\(\s*([^,)]+)(?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            format_param = match.group(2) if match.group(2) else None
            return column, format_param
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to date object"""
        if value is None:
            return None

        if isinstance(value, date) and not isinstance(value, datetime):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            # Try ISO format first
            try:
                dt = datetime.fromisoformat(value)
                return dt.date()
            except (ValueError, TypeError):
                pass

            # Try custom format if provided
            if format_param:
                try:
                    dt = datetime.strptime(value, format_param)
                    return dt.date()
                except (ValueError, TypeError):
                    pass

            # Fallback: try common formats
            for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"]:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.date()
                except ValueError:
                    continue

        return value

    def get_type_code(self) -> str:
        return "date"


class TimestampFunction(ProjectionFunction):
    """TIMESTAMP(column [, 'format']) - Convert to BSON Timestamp"""

    function_name = "TIMESTAMP"

    def can_handle(self, text: str) -> bool:
        """Check if text is a TIMESTAMP() function call"""
        return bool(re.match(r"^\s*TIMESTAMP\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column and optional format from TIMESTAMP(column [, 'format'])"""
        # Pattern: TIMESTAMP(column_expr [, 'format'])
        match = re.match(r"^\s*TIMESTAMP\s*\(\s*([^,)]+)(?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            format_param = match.group(2) if match.group(2) else None
            return column, format_param
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to BSON Timestamp"""
        if value is None:
            return None

        if isinstance(value, Timestamp):
            return value

        if isinstance(value, (int, float)):
            # Unix timestamp
            return Timestamp(int(value), 0)

        if isinstance(value, str):
            # Try ISO format first
            try:
                dt = datetime.fromisoformat(value)
                timestamp_int = int(dt.timestamp())
                return Timestamp(timestamp_int, 0)
            except (ValueError, TypeError):
                pass

            # Try date-only format (YYYY-MM-DD)
            if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
                try:
                    dt = datetime.strptime(value, "%Y-%m-%d")
                    timestamp_int = int(dt.timestamp())
                    return Timestamp(timestamp_int, 0)
                except ValueError:
                    pass

            # Try custom format if provided
            if format_param:
                try:
                    dt = datetime.strptime(value, format_param)
                    timestamp_int = int(dt.timestamp())
                    return Timestamp(timestamp_int, 0)
                except ValueError:
                    pass

            # Try common formats
            for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"]:
                try:
                    dt = datetime.strptime(value, fmt)
                    timestamp_int = int(dt.timestamp())
                    return Timestamp(timestamp_int, 0)
                except ValueError:
                    continue

        return value

    def get_type_code(self) -> str:
        return "timestamp"


class DatetimeFunction(ProjectionFunction):
    """DATETIME(column [, 'format']) - Convert to datetime"""

    function_name = "DATETIME"

    def can_handle(self, text: str) -> bool:
        """Check if text is a DATETIME() function call"""
        return bool(re.match(r"^\s*DATETIME\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column and optional format from DATETIME(column [, 'format'])"""
        # Pattern: DATETIME(column_expr [, 'format'])
        match = re.match(r"^\s*DATETIME\s*\(\s*([^,)]+)(?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            format_param = match.group(2) if match.group(2) else None
            return column, format_param
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to datetime object"""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            # Try ISO format first
            try:
                return datetime.fromisoformat(value)
            except (ValueError, TypeError):
                pass

            # Try custom format if provided
            if format_param:
                try:
                    return datetime.strptime(value, format_param)
                except (ValueError, TypeError):
                    pass

            # Try common formats
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

        return value

    def get_type_code(self) -> str:
        return "datetime"


class NumberFunction(ProjectionFunction):
    """NUMBER(column) - Convert to float"""

    function_name = "NUMBER"

    def can_handle(self, text: str) -> bool:
        """Check if text is a NUMBER() function call"""
        return bool(re.match(r"^\s*NUMBER\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column from NUMBER(column)"""
        match = re.match(r"^\s*NUMBER\s*\(\s*([^)]+)\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            return column, None
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to float"""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                pass

        return value

    def get_type_code(self) -> str:
        return "float"


class BoolFunction(ProjectionFunction):
    """BOOL(column) - Convert to bool"""

    function_name = "BOOL"

    def can_handle(self, text: str) -> bool:
        """Check if text is a BOOL() function call"""
        return bool(re.match(r"^\s*BOOL\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column from BOOL(column)"""
        match = re.match(r"^\s*BOOL\s*\(\s*([^)]+)\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            return column, None
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert to bool"""
        if value is None:
            return None

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return bool(value)

        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")

        return bool(value)

    def get_type_code(self) -> str:
        return "bool"


class SubstrFunction(ProjectionFunction):
    """SUBSTR(column, start [, length]) - Extract substring"""

    function_name = "SUBSTR"

    def can_handle(self, text: str) -> bool:
        """Check if text is a SUBSTR() function call"""
        return bool(re.match(r"^\s*SUBSTR(?:ING)?\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column and parameters from SUBSTR(column, start [, length])"""
        # Pattern: SUBSTR(column, start) or SUBSTR(column, start, length)
        match = re.match(
            r"^\s*SUBSTR(?:ING)?\s*\(\s*([^,]+)\s*,\s*([^,)]+)(?:\s*,\s*([^)]+))?\s*\)\s*$", text, re.IGNORECASE
        )
        if match:
            column = match.group(1).strip()
            start = match.group(2).strip()
            length = match.group(3).strip() if match.group(3) else None
            # Store start and length in format_param as JSON-like string
            params = f"{start},{length}" if length else start
            return column, params
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Extract substring from string"""
        if value is None or not isinstance(value, str):
            return value

        if not format_param:
            return value

        try:
            parts = format_param.split(",", 1)
            start = int(parts[0].strip())
            length = int(parts[1].strip()) if len(parts) > 1 else None

            # Convert 1-based SQL index to 0-based Python index
            start = max(0, start - 1)

            if length is not None:
                return value[start : start + length]
            else:
                return value[start:]
        except (ValueError, IndexError):
            return value

    def get_type_code(self) -> str:
        return "str"


class SubstringFunction(SubstrFunction):
    """SUBSTRING(column, start [, length]) - Same as SUBSTR"""

    function_name = "SUBSTRING"

    def can_handle(self, text: str) -> bool:
        """Check if text is a SUBSTRING() function call"""
        return bool(re.match(r"^\s*SUBSTRING\s*\(", text, re.IGNORECASE))


class ReplaceFunction(ProjectionFunction):
    """REPLACE(column, pattern [, replacement]) - Replace substring"""

    function_name = "REPLACE"

    def can_handle(self, text: str) -> bool:
        """Check if text is a REPLACE() function call"""
        return bool(re.match(r"^\s*REPLACE\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column and parameters from REPLACE(column, pattern [, replacement])"""
        # Pattern: REPLACE(column, 'pattern') or REPLACE(column, 'pattern', 'replacement')
        match = re.match(
            r"^\s*REPLACE\s*\(\s*([^,]+)\s*,\s*['\"]([^'\"]*)['\"](?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*$",
            text,
            re.IGNORECASE,
        )
        if match:
            column = match.group(1).strip()
            pattern = match.group(2)
            replacement = match.group(3) if match.group(3) is not None else ""
            # Store pattern and replacement in format_param
            params = f"{pattern}|{replacement}"
            return column, params
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Replace pattern in string"""
        if value is None or not isinstance(value, str):
            return value

        if not format_param or "|" not in format_param:
            return value

        try:
            parts = format_param.split("|", 1)
            pattern = parts[0]
            replacement = parts[1] if len(parts) > 1 else ""
            return value.replace(pattern, replacement)
        except Exception:
            return value

    def get_type_code(self) -> str:
        return "str"


class TrimFunction(ProjectionFunction):
    """TRIM(column) - Remove leading and trailing whitespace"""

    function_name = "TRIM"

    def can_handle(self, text: str) -> bool:
        """Check if text is a TRIM() function call"""
        return bool(re.match(r"^\s*TRIM\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column from TRIM(column)"""
        match = re.match(r"^\s*TRIM\s*\(\s*([^)]+)\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            return column, None
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Trim whitespace from string"""
        if value is None or not isinstance(value, str):
            return value
        return value.strip()

    def get_type_code(self) -> str:
        return "str"


class UpperFunction(ProjectionFunction):
    """UPPER(column) - Convert to uppercase"""

    function_name = "UPPER"

    def can_handle(self, text: str) -> bool:
        """Check if text is an UPPER() function call"""
        return bool(re.match(r"^\s*UPPER\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column from UPPER(column)"""
        match = re.match(r"^\s*UPPER\s*\(\s*([^)]+)\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            return column, None
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert string to uppercase"""
        if value is None or not isinstance(value, str):
            return value
        return value.upper()

    def get_type_code(self) -> str:
        return "str"


class LowerFunction(ProjectionFunction):
    """LOWER(column) - Convert to lowercase"""

    function_name = "LOWER"

    def can_handle(self, text: str) -> bool:
        """Check if text is a LOWER() function call"""
        return bool(re.match(r"^\s*LOWER\s*\(", text, re.IGNORECASE))

    def extract_column_and_format(self, text: str) -> Tuple[str, Optional[str]]:
        """Extract column from LOWER(column)"""
        match = re.match(r"^\s*LOWER\s*\(\s*([^)]+)\s*\)\s*$", text, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            return column, None
        return "", None

    def convert_value(self, value: Any, format_param: Optional[str] = None) -> Any:
        """Convert string to lowercase"""
        if value is None or not isinstance(value, str):
            return value
        return value.lower()

    def get_type_code(self) -> str:
        return "str"


class ProjectionFunctionRegistry:
    """Registry for projection functions"""

    _instance = None
    _functions: List[ProjectionFunction] = []

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize with built-in functions"""
        self._functions = [
            # Conversion functions
            DateFunction(),
            DatetimeFunction(),
            TimestampFunction(),
            NumberFunction(),
            BoolFunction(),
            # String functions
            SubstrFunction(),
            SubstringFunction(),
            ReplaceFunction(),
            TrimFunction(),
            UpperFunction(),
            LowerFunction(),
        ]

    def find_function(self, text: str) -> Optional[ProjectionFunction]:
        """Find function handler for the given text"""
        for func in self._functions:
            if func.can_handle(text):
                return func
        return None

    def register_function(self, func: ProjectionFunction) -> None:
        """Register a custom projection function"""
        if not isinstance(func, ProjectionFunction):
            raise TypeError(f"Function must inherit from ProjectionFunction, got {type(func)}")
        self._functions.append(func)

    def get_all_functions(self) -> List[ProjectionFunction]:
        """Get all registered functions"""
        return self._functions.copy()
