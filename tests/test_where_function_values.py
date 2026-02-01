# -*- coding: utf-8 -*-
from datetime import date, datetime, timezone

from bson import Timestamp

from pymongosql.sql.handler import ComparisonExpressionHandler, OperatorExtractorMixin


class TestFunctionValueParsing:
    """Test parsing of function calls in WHERE condition values"""

    def test_date_function_in_where_value(self):
        """Test date('2025-01-01') in WHERE clause"""
        handler = OperatorExtractorMixin()

        # Parse date function call
        result = handler._parse_value("date('2025-01-01')")

        assert isinstance(result, date)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 1

    def test_datetime_function_in_where_value(self):
        """Test datetime('2025-01-15T10:30:00') in WHERE clause"""
        handler = OperatorExtractorMixin()

        result = handler._parse_value("datetime('2025-01-15T10:30:00')")

        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_datetime_function_with_z_suffix(self):
        """Test datetime with Z suffix"""
        handler = OperatorExtractorMixin()

        result = handler._parse_value("datetime('2025-01-15T10:30:00Z')")

        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.hour == 10

    def test_timestamp_function_in_where_value(self):
        """Test timestamp('2025-01-15T10:30:00Z') in WHERE clause"""
        handler = OperatorExtractorMixin()

        result = handler._parse_value("timestamp('2025-01-15T10:30:00Z')")

        assert isinstance(result, Timestamp)

    def test_date_function_with_double_quotes(self):
        """Test date function with double quotes"""
        handler = OperatorExtractorMixin()

        result = handler._parse_value('date("2025-01-01")')

        assert isinstance(result, date)
        assert result.year == 2025

    def test_date_function_case_insensitive(self):
        """Test that function names are case-insensitive"""
        handler = OperatorExtractorMixin()

        result1 = handler._parse_value("DATE('2025-01-01')")
        result2 = handler._parse_value("date('2025-01-01')")
        result3 = handler._parse_value("Date('2025-01-01')")

        assert isinstance(result1, date)
        assert isinstance(result2, date)
        assert isinstance(result3, date)
        assert result1 == result2 == result3

    def test_number_function_not_supported_in_where_clause(self):
        """Test that number() function is NOT supported in WHERE clauses

        WHERE clause functions only support: DATE, DATETIME, TIMESTAMP
        number() and bool() are projection-only functions
        """
        handler = OperatorExtractorMixin()

        # number() falls back to string parsing since it's not in WhereClauseFunctionRegistry
        result = handler._parse_value("number('42.5')")

        # Should be treated as a string (not a function)
        assert isinstance(result, str)

    def test_bool_function_not_supported_in_where_clause(self):
        """Test that bool() function is NOT supported in WHERE clauses

        WHERE clause functions only support: DATE, DATETIME, TIMESTAMP
        number() and bool() are projection-only functions
        """
        handler = OperatorExtractorMixin()

        # bool() falls back to string parsing since it's not in WhereClauseFunctionRegistry
        result = handler._parse_value("bool('true')")

        # Should be treated as a string (not a function)
        assert isinstance(result, str)

    def test_non_function_string_still_works(self):
        """Test that non-function strings are still parsed normally"""
        handler = OperatorExtractorMixin()

        result = handler._parse_value("'hello world'")

        assert result == "hello world"
        assert isinstance(result, str)

    def test_number_value_still_works(self):
        """Test that numeric values are still parsed normally"""
        handler = OperatorExtractorMixin()

        result = handler._parse_value("123")

        assert result == 123
        assert isinstance(result, int)

    def test_null_still_works(self):
        """Test that NULL is still parsed normally"""
        handler = OperatorExtractorMixin()

        result = handler._parse_value("NULL")

        assert result is None


class TestComparisonWithDateFunction:
    """Test comparison expressions with date functions"""

    def test_comparison_greater_than_date(self):
        """Test created_at > date('2025-01-01')"""
        handler = ComparisonExpressionHandler()

        # Simulate handler behavior
        field_name = "created_at"
        operator = ">"
        value = handler._parse_value("date('2025-01-01')")

        mongo_filter = handler._build_mongo_filter(field_name, operator, value)

        assert field_name in mongo_filter
        assert "$gt" in mongo_filter[field_name]
        assert isinstance(mongo_filter[field_name]["$gt"], date)

    def test_comparison_less_than_datetime(self):
        """Test event_time < datetime('2025-12-31T23:59:59Z')"""
        handler = ComparisonExpressionHandler()

        field_name = "event_time"
        operator = "<"
        value = handler._parse_value("datetime('2025-12-31T23:59:59Z')")

        mongo_filter = handler._build_mongo_filter(field_name, operator, value)

        assert field_name in mongo_filter
        assert "$lt" in mongo_filter[field_name]
        assert isinstance(mongo_filter[field_name]["$lt"], datetime)

    def test_comparison_equal_timestamp(self):
        """Test _ts = timestamp('2025-01-15T10:30:00Z')"""
        handler = ComparisonExpressionHandler()

        field_name = "_ts"
        operator = "="
        value = handler._parse_value("timestamp('2025-01-15T10:30:00Z')")

        mongo_filter = handler._build_mongo_filter(field_name, operator, value)

        assert field_name in mongo_filter
        assert isinstance(mongo_filter[field_name], Timestamp)

    def test_between_with_date_functions(self):
        """Test BETWEEN with date() functions"""
        handler = ComparisonExpressionHandler()

        field_name = "created_at"
        operator = "BETWEEN"
        # In BETWEEN, the value is a tuple of (start, end)
        start_value = handler._parse_value("date('2025-01-01')")
        end_value = handler._parse_value("date('2025-12-31')")
        value = (start_value, end_value)

        mongo_filter = handler._build_mongo_filter(field_name, operator, value)

        assert "$and" in mongo_filter
        assert len(mongo_filter["$and"]) == 2
        assert "$gte" in mongo_filter["$and"][0][field_name]
        assert "$lte" in mongo_filter["$and"][1][field_name]


class TestInClauseWithDateFunctions:
    """Test IN clause with date functions"""

    def test_in_with_date_values(self):
        """Test IN clause with date() values"""
        handler = OperatorExtractorMixin()

        # Simulate IN clause: order_date IN (date('2025-01-01'), date('2025-06-01'), date('2025-12-31'))
        values = [
            handler._parse_value("date('2025-01-01')"),
            handler._parse_value("date('2025-06-01')"),
            handler._parse_value("date('2025-12-31')"),
        ]

        assert len(values) == 3
        assert all(isinstance(v, date) for v in values)
        assert values[0].month == 1
        assert values[1].month == 6
        assert values[2].month == 12


class TestDateFormatVariations:
    """Test different date format support in functions"""

    def test_iso_date_format(self):
        """Test ISO date format: 2025-01-15"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("date('2025-01-15')")
        assert isinstance(result, date)
        assert result.year == 2025

    def test_iso_datetime_format(self):
        """Test ISO datetime format: 2025-01-15T10:30:00"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("datetime('2025-01-15T10:30:00')")
        assert isinstance(result, datetime)
        assert result.hour == 10

    def test_iso_datetime_with_z(self):
        """Test ISO datetime with Z: 2025-01-15T10:30:00Z"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("datetime('2025-01-15T10:30:00Z')")
        assert isinstance(result, datetime)

    def test_us_date_format(self):
        """Test US date format: 01/15/2025"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("date('01/15/2025')")
        assert isinstance(result, date)
        assert result.year == 2025

    def test_eu_date_format(self):
        """Test EU date format: 15/01/2025"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("date('15/01/2025')")
        assert isinstance(result, date)
        assert result.year == 2025

    def test_space_separated_datetime(self):
        """Test space-separated datetime: 2025-01-15 10:30:00"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("datetime('2025-01-15 10:30:00')")
        assert isinstance(result, datetime)
        assert result.hour == 10


class TestErrorHandling:
    """Test error handling for invalid date functions"""

    def test_invalid_date_string(self):
        """Test that invalid date strings return the unparsed value"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("date('invalid-date')")
        # Should return original string if parsing fails
        assert result == "invalid-date" or isinstance(result, str)

    def test_unknown_function_falls_back(self):
        """Test that unknown functions fall back to normal string parsing"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("custom_func('value')")
        # Should be treated as a regular string since custom_func doesn't exist
        assert isinstance(result, str)

    def test_empty_function_argument(self):
        """Test function with empty argument"""
        handler = OperatorExtractorMixin()
        result = handler._parse_value("date('')")
        # Empty string parsing might fail, which is expected
        assert result is not None  # Just verify it doesn't crash


class TestRealWorldScenarios:
    """Test real-world query scenarios"""

    def test_users_created_after_date(self):
        """SELECT * FROM users WHERE created_at > date('2025-01-01')

        Note: date() in WHERE clause returns datetime with UTC timezone for BSON compatibility.
        MongoDB BSON Date requires datetime objects, not date objects.
        """
        handler = ComparisonExpressionHandler()

        field_name = "created_at"
        operator = ">"
        value = handler._parse_value("date('2025-01-01')")

        mongo_filter = handler._build_mongo_filter(field_name, operator, value)

        # date() in WHERE returns datetime with UTC timezone for BSON compatibility
        expected_dt = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert mongo_filter == {"created_at": {"$gt": expected_dt}}

    def test_orders_between_dates(self):
        """SELECT * FROM orders WHERE order_date BETWEEN date('2025-01-01') AND date('2025-12-31')"""
        handler = ComparisonExpressionHandler()

        field_name = "order_date"
        start = handler._parse_value("date('2025-01-01')")
        end = handler._parse_value("date('2025-12-31')")

        mongo_filter = handler._build_mongo_filter(field_name, "BETWEEN", (start, end))

        assert "$and" in mongo_filter
        # Should have two conditions: $gte and $lte

    def test_events_at_specific_time(self):
        """SELECT * FROM events WHERE event_time = datetime('2025-01-15T10:30:00Z')"""
        handler = ComparisonExpressionHandler()

        field_name = "event_time"
        operator = "="
        value = handler._parse_value("datetime('2025-01-15T10:30:00Z')")

        mongo_filter = handler._build_mongo_filter(field_name, operator, value)

        assert isinstance(mongo_filter[field_name], datetime)

    def test_logs_with_specific_timestamps(self):
        """SELECT * FROM logs WHERE _ts IN (timestamp('2025-01-01T00:00:00Z'), timestamp('2025-06-01T00:00:00Z'))"""
        handler = OperatorExtractorMixin()

        values = [
            handler._parse_value("timestamp('2025-01-01T00:00:00Z')"),
            handler._parse_value("timestamp('2025-06-01T00:00:00Z')"),
        ]

        assert all(isinstance(v, Timestamp) for v in values)
