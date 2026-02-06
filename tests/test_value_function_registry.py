# -*- coding: utf-8 -*-
from datetime import datetime, timezone

import pytest
from bson.timestamp import Timestamp

from pymongosql.sql.value_function_registry import (
    ValueFunctionExecutionError,
    ValueFunctionRegistry,
)


@pytest.fixture
def registry():
    """Fixture providing a fresh ValueFunctionRegistry for each test"""
    return ValueFunctionRegistry()


class TestValueFunctionRegistry:
    """Test cases for ValueFunctionRegistry"""

    def test_registry_initialization(self, registry):
        """Test that registry initializes with built-in functions"""
        assert registry.has_function("str_to_datetime")
        assert registry.has_function("str_to_timestamp")

    def test_list_functions(self, registry):
        """Test listing registered functions"""
        functions = registry.list_functions()
        assert "str_to_datetime" in functions
        assert "str_to_timestamp" in functions

    def test_function_case_insensitive(self, registry):
        """Test that function names are case-insensitive"""
        assert registry.has_function("STR_TO_DATETIME")
        assert registry.has_function("Str_To_Datetime")
        assert registry.has_function("STR_TO_TIMESTAMP")


class TestDatetimeFunction:
    """Test cases for str_to_datetime() function"""

    def test_datetime_iso8601_basic(self, registry):
        """Test datetime conversion from ISO 8601 format"""
        result = registry.execute("str_to_datetime", ["2024-01-15"])
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.tzinfo == timezone.utc

    def test_datetime_iso8601_with_time(self, registry):
        """Test datetime conversion from ISO 8601 with time"""
        result = registry.execute("str_to_datetime", ["2024-01-15T10:30:45"])
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 45
        assert result.tzinfo == timezone.utc

    def test_datetime_iso8601_with_z(self, registry):
        """Test datetime conversion from ISO 8601 with Z timezone"""
        result = registry.execute("str_to_datetime", ["2024-01-15T10:30:45Z"])
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 45

    def test_datetime_custom_format(self, registry):
        """Test datetime conversion with custom format"""
        result = registry.execute("str_to_datetime", ["01/15/2024", "%m/%d/%Y"])
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.tzinfo == timezone.utc

    def test_datetime_custom_format_with_time(self, registry):
        """Test datetime conversion with custom format including time"""
        result = registry.execute("str_to_datetime", ["01/15/2024 10:30:45", "%m/%d/%Y %H:%M:%S"])
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.second == 45

    def test_datetime_invalid_format(self, registry):
        """Test datetime with invalid format raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_datetime", ["invalid-date"])

    def test_datetime_invalid_format_string(self, registry):
        """Test datetime with invalid format string raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_datetime", ["01/15/2024", "%Y-%m-%d"])  # Format mismatch

    def test_datetime_missing_argument(self, registry):
        """Test datetime with missing argument raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_datetime", [])

    def test_datetime_too_many_arguments(self, registry):
        """Test datetime with too many arguments raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_datetime", ["2024-01-15", "%Y-%m-%d", "extra"])

    def test_datetime_non_string_value(self, registry):
        """Test datetime with non-string value raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_datetime", [12345])

    def test_datetime_whitespace_handling(self, registry):
        """Test datetime handles whitespace in input"""
        result = registry.execute("str_to_datetime", ["  2024-01-15  "])
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15


class TestTimestampFunction:
    """Test cases for str_to_timestamp() function"""

    def test_timestamp_iso8601_basic(self, registry):
        """Test timestamp conversion from ISO 8601 format"""
        result = registry.execute("str_to_timestamp", ["2024-01-15"])
        assert isinstance(result, Timestamp)
        assert result.time is not None
        assert result.inc is not None

    def test_timestamp_iso8601_with_time(self, registry):
        """Test timestamp conversion from ISO 8601 with time"""
        result = registry.execute("str_to_timestamp", ["2024-01-15T10:30:45"])
        assert isinstance(result, Timestamp)
        assert result.time is not None

    def test_timestamp_iso8601_with_z(self, registry):
        """Test timestamp conversion from ISO 8601 with Z timezone"""
        result = registry.execute("str_to_timestamp", ["2024-01-15T10:30:45Z"])
        assert isinstance(result, Timestamp)
        assert result.time is not None

    def test_timestamp_custom_format(self, registry):
        """Test timestamp conversion with custom format"""
        result = registry.execute("str_to_timestamp", ["01/15/2024", "%m/%d/%Y"])
        assert isinstance(result, Timestamp)
        assert result.time is not None

    def test_timestamp_custom_format_with_time(self, registry):
        """Test timestamp conversion with custom format including time"""
        result = registry.execute("str_to_timestamp", ["01/15/2024 10:30:45", "%m/%d/%Y %H:%M:%S"])
        assert isinstance(result, Timestamp)
        assert result.time is not None

    def test_timestamp_increment_value(self, registry):
        """Test timestamp has increment value of 1"""
        result = registry.execute("str_to_timestamp", ["2024-01-15"])
        assert result.inc == 1

    def test_timestamp_invalid_format(self, registry):
        """Test timestamp with invalid format raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_timestamp", ["invalid-date"])

    def test_timestamp_missing_argument(self, registry):
        """Test timestamp with missing argument raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_timestamp", [])

    def test_timestamp_too_many_arguments(self, registry):
        """Test timestamp with too many arguments raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_timestamp", ["2024-01-15", "%Y-%m-%d", "extra"])

    def test_timestamp_non_string_value(self, registry):
        """Test timestamp with non-string value raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_timestamp", [12345])


class TestCustomFunctionRegistration:
    """Test cases for registering custom functions"""

    def test_register_custom_function(self, registry):
        """Test registering a custom function"""

        def custom_upper(val):
            return val.upper() if isinstance(val, str) else str(val).upper()

        registry.register("upper", custom_upper)
        assert registry.has_function("upper")

    def test_execute_custom_function(self, registry):
        """Test executing a custom function"""

        def custom_upper(val):
            return val.upper() if isinstance(val, str) else str(val).upper()

        registry.register("upper", custom_upper)
        result = registry.execute("upper", ["hello"])
        assert result == "HELLO"

    def test_register_invalid_function_name(self, registry):
        """Test registering with invalid function name raises error"""

        def dummy():
            pass

        with pytest.raises(ValueError):
            registry.register("", dummy)

        with pytest.raises(ValueError):
            registry.register(None, dummy)

    def test_register_non_callable(self, registry):
        """Test registering non-callable raises error"""
        with pytest.raises(ValueError):
            registry.register("notfunc", "not a function")

    def test_unregister_function(self, registry):
        """Test unregistering a function"""

        def dummy():
            pass

        registry.register("temp", dummy)
        assert registry.has_function("temp")
        registry.unregister("temp")
        assert not registry.has_function("temp")

    def test_unregister_nonexistent_function(self, registry):
        """Test unregistering non-existent function doesn't raise error"""
        # Should not raise
        registry.unregister("nonexistent")

    def test_overwrite_existing_function(self, registry):
        """Test overwriting an existing function"""

        def func1():
            return 1

        def func2():
            return 2

        registry.register("test", func1)
        result1 = registry.execute("test", [])
        assert result1 == 1

        registry.register("test", func2)
        result2 = registry.execute("test", [])
        assert result2 == 2


class TestFunctionExecutionErrors:
    """Test error handling in function execution"""

    def test_nonexistent_function(self, registry):
        """Test executing non-existent function raises error"""
        with pytest.raises(ValueFunctionExecutionError) as exc_info:
            registry.execute("nonexistent", [])
        assert "nonexistent" in str(exc_info.value)

    def test_function_with_wrong_argument_count(self, registry):
        """Test executing function with wrong argument count raises error"""
        with pytest.raises(ValueFunctionExecutionError):
            registry.execute("str_to_datetime", ["2024-01-15", "extra", "args"])
