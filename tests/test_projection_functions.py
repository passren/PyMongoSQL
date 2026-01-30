# -*- coding: utf-8 -*-
"""Tests for projection functions"""

import re
from datetime import date, datetime

from bson import Timestamp

from pymongosql.sql.projection_functions import (
    BoolFunction,
    DateFunction,
    DatetimeFunction,
    LowerFunction,
    NumberFunction,
    ProjectionFunction,
    ProjectionFunctionRegistry,
    ReplaceFunction,
    SubstrFunction,
    SubstringFunction,
    TimestampFunction,
    TrimFunction,
    UpperFunction,
)


class TestDateFunction:
    """Test DATE projection function"""

    def test_can_handle_date_function(self):
        """Test that DATE() function is recognized"""
        func = DateFunction()
        assert func.can_handle("DATE(created_at)")
        assert func.can_handle("DATE(created_at, '%Y-%m-%d')")
        assert func.can_handle("date(column_name)")
        assert not func.can_handle("TIMESTAMP(column)")

    def test_extract_column_and_format(self):
        """Test extraction of column and format parameters"""
        func = DateFunction()

        # Without format
        col, fmt = func.extract_column_and_format("DATE(created_at)")
        assert col == "created_at"
        assert fmt is None

        # With format
        col, fmt = func.extract_column_and_format("DATE(created_at, '%Y-%m-%d')")
        assert col == "created_at"
        assert fmt == "%Y-%m-%d"

    def test_convert_date_string(self):
        """Test conversion from date strings"""
        func = DateFunction()

        # ISO format
        result = func.convert_value("2025-01-15")
        assert isinstance(result, date)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

        # Datetime string
        result = func.convert_value("2025-01-15T10:30:00Z")
        assert isinstance(result, date)
        assert result.year == 2025

    def test_convert_datetime_object(self):
        """Test conversion from datetime objects"""
        func = DateFunction()
        dt = datetime(2025, 1, 15, 10, 30, 0)
        result = func.convert_value(dt)
        assert isinstance(result, date)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

    def test_convert_date_object(self):
        """Test that date objects are returned as-is"""
        func = DateFunction()
        d = date(2025, 1, 15)
        result = func.convert_value(d)
        assert result == d

    def test_convert_none(self):
        """Test that None is returned as None"""
        func = DateFunction()
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for date function"""
        func = DateFunction()
        assert func.get_type_code() == "date"


class TestDatetimeFunction:
    """Test DATETIME projection function"""

    def test_can_handle_datetime_function(self):
        """Test that DATETIME() function is recognized"""
        func = DatetimeFunction()
        assert func.can_handle("DATETIME(created_at)")
        assert func.can_handle("DATETIME(created_at, '%Y-%m-%d %H:%M:%S')")
        assert func.can_handle("datetime(column_name)")
        assert not func.can_handle("DATE(column)")

    def test_extract_column_and_format(self):
        """Test extraction of column and format parameters"""
        func = DatetimeFunction()

        # Without format
        col, fmt = func.extract_column_and_format("DATETIME(created_at)")
        assert col == "created_at"
        assert fmt is None

        # With format
        col, fmt = func.extract_column_and_format("DATETIME(created_at, '%Y-%m-%d %H:%M:%S')")
        assert col == "created_at"
        assert fmt == "%Y-%m-%d %H:%M:%S"

    def test_convert_iso_datetime_string(self):
        """Test conversion from ISO datetime strings"""
        func = DatetimeFunction()
        result = func.convert_value("2025-01-15T10:30:00")
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_convert_iso_datetime_string_with_z(self):
        """Test conversion from ISO datetime strings with Z"""
        func = DatetimeFunction()
        result = func.convert_value("2025-01-15T10:30:00Z")
        assert isinstance(result, datetime)

    def test_convert_datetime_with_custom_format(self):
        """Test conversion with custom format"""
        func = DatetimeFunction()
        result = func.convert_value("15/01/2025 10:30:00", "%d/%m/%Y %H:%M:%S")
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

    def test_convert_datetime_object(self):
        """Test that datetime objects are returned as-is"""
        func = DatetimeFunction()
        dt = datetime(2025, 1, 15, 10, 30, 0)
        result = func.convert_value(dt)
        assert result == dt

    def test_convert_date_only_string(self):
        """Test conversion from date-only strings"""
        func = DatetimeFunction()
        result = func.convert_value("2025-01-15")
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 0
        assert result.minute == 0

    def test_convert_none(self):
        """Test that None is returned as None"""
        func = DatetimeFunction()
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for datetime function"""
        func = DatetimeFunction()
        assert func.get_type_code() == "datetime"


class TestTimestampFunction:
    """Test TIMESTAMP projection function"""

    def test_can_handle_timestamp_function(self):
        """Test that TIMESTAMP() function is recognized"""
        func = TimestampFunction()
        assert func.can_handle("TIMESTAMP(created_at)")
        assert func.can_handle("TIMESTAMP(created_at, '%Y-%m-%d')")
        assert func.can_handle("timestamp(column_name)")
        assert not func.can_handle("DATE(column)")

    def test_extract_column_and_format(self):
        """Test extraction of column and format parameters"""
        func = TimestampFunction()

        # Without format
        col, fmt = func.extract_column_and_format("TIMESTAMP(created_at)")
        assert col == "created_at"
        assert fmt is None

        # With format
        col, fmt = func.extract_column_and_format("TIMESTAMP(created_at, '%Y-%m-%d')")
        assert col == "created_at"
        assert fmt == "%Y-%m-%d"

    def test_convert_iso_date_string(self):
        """Test conversion from ISO date strings"""
        func = TimestampFunction()
        result = func.convert_value("2025-01-15")
        assert isinstance(result, Timestamp)
        assert result.time == 1736917200  # Unix timestamp for 2025-01-15 00:00:00 UTC

    def test_convert_iso_datetime_string(self):
        """Test conversion from ISO datetime strings"""
        func = TimestampFunction()
        result = func.convert_value("2025-01-15T10:30:00Z")
        assert isinstance(result, Timestamp)

    def test_convert_unix_timestamp(self):
        """Test conversion from Unix timestamps"""
        func = TimestampFunction()
        result = func.convert_value(1736917200)
        assert isinstance(result, Timestamp)
        assert result.time == 1736917200

    def test_convert_timestamp_object(self):
        """Test that Timestamp objects are returned as-is"""
        func = TimestampFunction()
        ts = Timestamp(1736917200, 0)
        result = func.convert_value(ts)
        assert result == ts

    def test_convert_none(self):
        """Test that None is returned as None"""
        func = TimestampFunction()
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for timestamp function"""
        func = TimestampFunction()
        assert func.get_type_code() == "timestamp"


class TestNumberFunction:
    """Test NUMBER projection function"""

    def test_can_handle_number_function(self):
        """Test that NUMBER() function is recognized"""
        func = NumberFunction()
        assert func.can_handle("NUMBER(price)")
        assert func.can_handle("number(amount)")
        assert not func.can_handle("DATE(column)")

    def test_extract_column(self):
        """Test extraction of column"""
        func = NumberFunction()
        col, fmt = func.extract_column_and_format("NUMBER(price)")
        assert col == "price"
        assert fmt is None

    def test_convert_number_string(self):
        """Test conversion from number strings"""
        func = NumberFunction()
        assert func.convert_value("42") == 42.0
        assert func.convert_value("42.5") == 42.5

    def test_convert_int(self):
        """Test conversion from integers"""
        func = NumberFunction()
        assert func.convert_value(42) == 42.0

    def test_convert_float(self):
        """Test that floats are returned as-is"""
        func = NumberFunction()
        assert func.convert_value(42.5) == 42.5

    def test_convert_none(self):
        """Test that None is returned as None"""
        func = NumberFunction()
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for number function"""
        func = NumberFunction()
        assert func.get_type_code() == "float"


class TestBoolFunction:
    """Test BOOL projection function"""

    def test_can_handle_bool_function(self):
        """Test that BOOL() function is recognized"""
        func = BoolFunction()
        assert func.can_handle("BOOL(is_active)")
        assert func.can_handle("bool(enabled)")
        assert not func.can_handle("NUMBER(column)")

    def test_extract_column(self):
        """Test extraction of column"""
        func = BoolFunction()
        col, fmt = func.extract_column_and_format("BOOL(is_active)")
        assert col == "is_active"
        assert fmt is None

    def test_convert_bool_string(self):
        """Test conversion from boolean strings"""
        func = BoolFunction()
        assert func.convert_value("true") is True
        assert func.convert_value("TRUE") is True
        assert func.convert_value("1") is True
        assert func.convert_value("yes") is True
        assert func.convert_value("false") is False
        assert func.convert_value("0") is False

    def test_convert_numeric(self):
        """Test conversion from numeric values"""
        func = BoolFunction()
        assert func.convert_value(1) is True
        assert func.convert_value(0) is False
        assert func.convert_value(42) is True

    def test_convert_bool(self):
        """Test that bools are returned as-is"""
        func = BoolFunction()
        assert func.convert_value(True) is True
        assert func.convert_value(False) is False

    def test_convert_none(self):
        """Test that None is returned as None"""
        func = BoolFunction()
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for bool function"""
        func = BoolFunction()
        assert func.get_type_code() == "bool"


class TestSubstrFunction:
    """Test SUBSTR and SUBSTRING projection functions"""

    def test_can_handle_substr_function(self):
        """Test that SUBSTR() function is recognized"""
        func = SubstrFunction()
        assert func.can_handle("SUBSTR(name, 1)")
        assert func.can_handle("SUBSTR(name, 1, 3)")
        assert func.can_handle("substr(text, 2, 5)")
        assert not func.can_handle("UPPER(column)")

    def test_can_handle_substring_function(self):
        """Test that SUBSTRING() function is recognized"""
        func = SubstringFunction()
        assert func.can_handle("SUBSTRING(name, 1)")
        assert func.can_handle("SUBSTRING(name, 1, 3)")
        assert not func.can_handle("SUBSTR(column)")

    def test_extract_column_with_length(self):
        """Test extraction of column and length"""
        func = SubstrFunction()
        col, params = func.extract_column_and_format("SUBSTR(name, 1, 3)")
        assert col == "name"
        assert params == "1,3"

    def test_extract_column_without_length(self):
        """Test extraction of column without length"""
        func = SubstrFunction()
        col, params = func.extract_column_and_format("SUBSTR(name, 2)")
        assert col == "name"
        assert params == "2"

    def test_convert_substr_with_length(self):
        """Test substring extraction with length"""
        func = SubstrFunction()
        result = func.convert_value("Hello World", "1,5")
        assert result == "Hello"

    def test_convert_substr_without_length(self):
        """Test substring extraction without length (from position to end)"""
        func = SubstrFunction()
        result = func.convert_value("Hello World", "7")
        assert result == "World"

    def test_convert_substr_zero_based_index(self):
        """Test that SQL 1-based indexing is converted to Python 0-based"""
        func = SubstrFunction()
        result = func.convert_value("Hello", "2,2")
        assert result == "el"  # Starting at position 2 (H=1, e=2)

    def test_convert_substr_non_string(self):
        """Test that non-strings are returned as-is"""
        func = SubstrFunction()
        assert func.convert_value(123, "1,2") == 123
        assert func.convert_value(None, "1,2") is None

    def test_get_type_code(self):
        """Test type code for substr function"""
        func = SubstrFunction()
        assert func.get_type_code() == "str"


class TestReplaceFunction:
    """Test REPLACE projection function"""

    def test_can_handle_replace_function(self):
        """Test that REPLACE() function is recognized"""
        func = ReplaceFunction()
        assert func.can_handle("REPLACE(name, 'John')")
        assert func.can_handle("REPLACE(name, 'old', 'new')")
        assert func.can_handle("replace(text, 'a', 'b')")
        assert not func.can_handle("UPPER(column)")

    def test_extract_pattern_only(self):
        """Test extraction with pattern only"""
        func = ReplaceFunction()
        col, params = func.extract_column_and_format("REPLACE(name, 'John')")
        assert col == "name"
        assert params == "John|"

    def test_extract_pattern_and_replacement(self):
        """Test extraction with pattern and replacement"""
        func = ReplaceFunction()
        col, params = func.extract_column_and_format("REPLACE(name, 'old', 'new')")
        assert col == "name"
        assert params == "old|new"

    def test_convert_replace_with_replacement(self):
        """Test string replacement"""
        func = ReplaceFunction()
        result = func.convert_value("Hello World", "World|Universe")
        assert result == "Hello Universe"

    def test_convert_replace_without_replacement(self):
        """Test string replacement with empty string"""
        func = ReplaceFunction()
        result = func.convert_value("Hello World", "World|")
        assert result == "Hello "

    def test_convert_replace_multiple_occurrences(self):
        """Test that all occurrences are replaced"""
        func = ReplaceFunction()
        result = func.convert_value("aaa", "a|b")
        assert result == "bbb"

    def test_convert_replace_non_string(self):
        """Test that non-strings are returned as-is"""
        func = ReplaceFunction()
        assert func.convert_value(123, "1|2") == 123
        assert func.convert_value(None, "a|b") is None

    def test_get_type_code(self):
        """Test type code for replace function"""
        func = ReplaceFunction()
        assert func.get_type_code() == "str"


class TestTrimFunction:
    """Test TRIM projection function"""

    def test_can_handle_trim_function(self):
        """Test that TRIM() function is recognized"""
        func = TrimFunction()
        assert func.can_handle("TRIM(name)")
        assert func.can_handle("trim(text)")
        assert not func.can_handle("UPPER(column)")

    def test_extract_column(self):
        """Test extraction of column"""
        func = TrimFunction()
        col, fmt = func.extract_column_and_format("TRIM(name)")
        assert col == "name"
        assert fmt is None

    def test_convert_trim_leading(self):
        """Test trimming leading whitespace"""
        func = TrimFunction()
        assert func.convert_value("  hello") == "hello"

    def test_convert_trim_trailing(self):
        """Test trimming trailing whitespace"""
        func = TrimFunction()
        assert func.convert_value("hello  ") == "hello"

    def test_convert_trim_both(self):
        """Test trimming both sides"""
        func = TrimFunction()
        assert func.convert_value("  hello world  ") == "hello world"

    def test_convert_trim_internal_whitespace(self):
        """Test that internal whitespace is preserved"""
        func = TrimFunction()
        assert func.convert_value("  hello  world  ") == "hello  world"

    def test_convert_trim_tabs_and_newlines(self):
        """Test trimming tabs and newlines"""
        func = TrimFunction()
        assert func.convert_value("\t\nhello\n\t") == "hello"

    def test_convert_trim_non_string(self):
        """Test that non-strings are returned as-is"""
        func = TrimFunction()
        assert func.convert_value(123) == 123
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for trim function"""
        func = TrimFunction()
        assert func.get_type_code() == "str"


class TestUpperFunction:
    """Test UPPER projection function"""

    def test_can_handle_upper_function(self):
        """Test that UPPER() function is recognized"""
        func = UpperFunction()
        assert func.can_handle("UPPER(name)")
        assert func.can_handle("upper(text)")
        assert not func.can_handle("LOWER(column)")

    def test_extract_column(self):
        """Test extraction of column"""
        func = UpperFunction()
        col, fmt = func.extract_column_and_format("UPPER(name)")
        assert col == "name"
        assert fmt is None

    def test_convert_uppercase(self):
        """Test conversion to uppercase"""
        func = UpperFunction()
        assert func.convert_value("hello") == "HELLO"
        assert func.convert_value("Hello World") == "HELLO WORLD"

    def test_convert_already_uppercase(self):
        """Test that already uppercase strings are unchanged"""
        func = UpperFunction()
        assert func.convert_value("HELLO") == "HELLO"

    def test_convert_mixed_case(self):
        """Test conversion of mixed case"""
        func = UpperFunction()
        assert func.convert_value("HeLLo WoRLd") == "HELLO WORLD"

    def test_convert_with_numbers_and_symbols(self):
        """Test that numbers and symbols are preserved"""
        func = UpperFunction()
        assert func.convert_value("hello123!@#") == "HELLO123!@#"

    def test_convert_non_string(self):
        """Test that non-strings are returned as-is"""
        func = UpperFunction()
        assert func.convert_value(123) == 123
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for upper function"""
        func = UpperFunction()
        assert func.get_type_code() == "str"


class TestLowerFunction:
    """Test LOWER projection function"""

    def test_can_handle_lower_function(self):
        """Test that LOWER() function is recognized"""
        func = LowerFunction()
        assert func.can_handle("LOWER(name)")
        assert func.can_handle("lower(text)")
        assert not func.can_handle("UPPER(column)")

    def test_extract_column(self):
        """Test extraction of column"""
        func = LowerFunction()
        col, fmt = func.extract_column_and_format("LOWER(name)")
        assert col == "name"
        assert fmt is None

    def test_convert_lowercase(self):
        """Test conversion to lowercase"""
        func = LowerFunction()
        assert func.convert_value("HELLO") == "hello"
        assert func.convert_value("Hello World") == "hello world"

    def test_convert_already_lowercase(self):
        """Test that already lowercase strings are unchanged"""
        func = LowerFunction()
        assert func.convert_value("hello") == "hello"

    def test_convert_mixed_case(self):
        """Test conversion of mixed case"""
        func = LowerFunction()
        assert func.convert_value("HeLLo WoRLd") == "hello world"

    def test_convert_with_numbers_and_symbols(self):
        """Test that numbers and symbols are preserved"""
        func = LowerFunction()
        assert func.convert_value("HELLO123!@#") == "hello123!@#"

    def test_convert_non_string(self):
        """Test that non-strings are returned as-is"""
        func = LowerFunction()
        assert func.convert_value(123) == 123
        assert func.convert_value(None) is None

    def test_get_type_code(self):
        """Test type code for lower function"""
        func = LowerFunction()
        assert func.get_type_code() == "str"


class TestProjectionFunctionRegistry:
    """Test projection function registry"""

    def test_singleton_instance(self):
        """Test that registry is a singleton"""
        registry1 = ProjectionFunctionRegistry()
        registry2 = ProjectionFunctionRegistry()
        assert registry1 is registry2

    def test_find_date_function(self):
        """Test finding DATE function"""
        registry = ProjectionFunctionRegistry()
        func = registry.find_function("DATE(created_at)")
        assert func is not None
        assert isinstance(func, DateFunction)

    def test_find_datetime_function(self):
        """Test finding DATETIME function"""
        registry = ProjectionFunctionRegistry()
        func = registry.find_function("DATETIME(created_at)")
        assert func is not None
        assert isinstance(func, DatetimeFunction)

    def test_find_timestamp_function(self):
        """Test finding TIMESTAMP function"""
        registry = ProjectionFunctionRegistry()
        func = registry.find_function("TIMESTAMP(created_at)")
        assert func is not None
        assert isinstance(func, TimestampFunction)

    def test_find_number_function(self):
        """Test finding NUMBER function"""
        registry = ProjectionFunctionRegistry()
        func = registry.find_function("NUMBER(price)")
        assert func is not None
        assert isinstance(func, NumberFunction)

    def test_find_bool_function(self):
        """Test finding BOOL function"""
        registry = ProjectionFunctionRegistry()
        func = registry.find_function("BOOL(is_active)")
        assert func is not None
        assert isinstance(func, BoolFunction)

    def test_find_nonexistent_function(self):
        """Test that None is returned for non-existent function"""
        registry = ProjectionFunctionRegistry()
        func = registry.find_function("CUSTOM(column)")
        assert func is None

    def test_get_all_functions(self):
        """Test getting all registered functions"""
        registry = ProjectionFunctionRegistry()
        functions = registry.get_all_functions()
        assert len(functions) >= 11  # 5 conversion + 6 string
        function_names = [f.function_name for f in functions]
        # Conversion functions
        assert "DATE" in function_names
        assert "DATETIME" in function_names
        assert "TIMESTAMP" in function_names
        assert "NUMBER" in function_names
        assert "BOOL" in function_names
        # String functions
        assert "SUBSTR" in function_names
        assert "SUBSTRING" in function_names
        assert "REPLACE" in function_names
        assert "TRIM" in function_names
        assert "UPPER" in function_names
        assert "LOWER" in function_names

    def test_register_custom_function(self):
        """Test registering a custom projection function"""
        registry = ProjectionFunctionRegistry()

        class CustomReverseFunction(ProjectionFunction):  # Unique name
            function_name = "REVERSE"

            def can_handle(self, text: str) -> bool:
                return text.upper().startswith("REVERSE(")

            def extract_column_and_format(self, text: str) -> tuple:
                match = re.match(r"^\s*REVERSE\s*\(\s*([^)]+)\s*\)\s*$", text, re.IGNORECASE)
                return (match.group(1).strip() if match else "", None)

            def convert_value(self, value, format_param=None):
                if isinstance(value, str):
                    return value[::-1]
                return value

            def get_type_code(self) -> str:
                return "str"

        registry.register_function(CustomReverseFunction())

        # Find the custom function
        func = registry.find_function("REVERSE(name)")
        assert func is not None
        assert isinstance(func, CustomReverseFunction)

        # Verify it works
        assert func.convert_value("hello") == "olleh"
