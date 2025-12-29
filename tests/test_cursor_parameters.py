# -*- coding: utf-8 -*-
import pytest

from pymongosql.executor import StandardQueryExecution


class TestPositionalParameters:
    """Test suite for positional parameters with ? placeholders"""

    def test_simple_positional_replacement(self):
        """Test basic positional parameter replacement in filter"""
        execution = StandardQueryExecution()

        test_filter = {"age": "?", "status": "?"}
        params = [25, "active"]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"age": 25, "status": "active"}

    def test_nested_positional_replacement(self):
        """Test positional parameter replacement in nested filter"""
        execution = StandardQueryExecution()

        test_filter = {"profile": {"age": "?"}, "status": "?"}
        params = [30, "inactive"]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"profile": {"age": 30}, "status": "inactive"}

    def test_list_positional_replacement(self):
        """Test positional parameter replacement in list"""
        execution = StandardQueryExecution()

        test_filter = {"items": ["?", "?"], "name": "?"}
        params = [1, 2, "test"]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"items": [1, 2], "name": "test"}

    def test_mixed_positional_replacement(self):
        """Test positional parameter replacement with mixed data types"""
        execution = StandardQueryExecution()

        test_filter = {"$gt": "?", "$lt": "?", "status": "?"}
        params = [18, 65, "active"]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"$gt": 18, "$lt": 65, "status": "active"}

    def test_insufficient_positional_parameters(self):
        """Test error when not enough positional parameters provided"""
        from pymongosql.error import ProgrammingError

        execution = StandardQueryExecution()

        test_filter = {"age": "?", "status": "?"}
        params = [25]  # Only one parameter provided

        with pytest.raises(ProgrammingError) as exc_info:
            execution._replace_placeholders(test_filter, params)

        assert "Not enough parameters" in str(exc_info.value)

    def test_complex_nested_positional_replacement(self):
        """Test positional parameters in complex nested structures"""
        execution = StandardQueryExecution()

        test_filter = {"$and": [{"age": {"$gt": "?"}}, {"profile": {"status": "?"}}, {"items": ["?", "?"]}]}
        params = [25, "active", 1, 2]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"$and": [{"age": {"$gt": 25}}, {"profile": {"status": "active"}}, {"items": [1, 2]}]}


class TestParameterTypes:
    """Test parameter handling with different data types"""

    def test_positional_with_numeric_types(self):
        """Test positional parameters with int and float"""
        execution = StandardQueryExecution()

        test_filter = {"age": "?", "salary": "?"}
        params = [25, 50000.50]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"age": 25, "salary": 50000.50}

    def test_positional_with_boolean(self):
        """Test positional parameters with boolean values"""
        execution = StandardQueryExecution()

        test_filter = {"active": "?", "verified": "?"}
        params = [True, False]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"active": True, "verified": False}

    def test_positional_with_null(self):
        """Test positional parameters with None value"""
        execution = StandardQueryExecution()

        test_filter = {"deleted_at": "?"}
        params = [None]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"deleted_at": None}

    def test_positional_with_list_value(self):
        """Test positional parameter with list as value"""
        execution = StandardQueryExecution()

        test_filter = {"tags": "?"}
        params = [["python", "mongodb"]]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"tags": ["python", "mongodb"]}

    def test_positional_with_dict_value(self):
        """Test positional parameter with dict as value"""
        execution = StandardQueryExecution()

        test_filter = {"metadata": "?"}
        params = [{"key": "value"}]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"metadata": {"key": "value"}}


class TestEdgeCases:
    """Test edge cases and special scenarios"""

    def test_empty_filter_with_parameters(self):
        """Test parameters with empty filter"""
        execution = StandardQueryExecution()

        result = execution._replace_placeholders({}, [])
        assert result == {}

    def test_non_placeholder_strings_untouched(self):
        """Test that non-placeholder strings are not modified"""
        execution = StandardQueryExecution()

        test_filter = {"status": "active", "query": "search"}
        params = [25, "test"]
        result = execution._replace_placeholders(test_filter, params)

        assert result == {"status": "active", "query": "search"}
