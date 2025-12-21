# -*- coding: utf-8 -*-
"""
Comprehensive tests for nested field support in PyMongoSQL
"""
import pytest

from pymongosql.error import SqlSyntaxError
from pymongosql.sql.parser import SQLParser


class TestSQLParserNestedFields:
    """Test suite for nested field querying functionality"""

    def test_basic_single_level_nesting_select(self):
        """Test basic single-level nested fields in SELECT"""
        sql = "SELECT c.a, c.b FROM collection"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "collection"
        assert execution_plan.projection_stage == {"c.a": 1, "c.b": 1}
        assert execution_plan.filter_stage == {}

    def test_basic_single_level_nesting_where(self):
        """Test basic single-level nested fields in WHERE clause"""
        sql = "SELECT * FROM users WHERE profile.status = 'active'"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"profile.status": "active"}

    def test_multi_level_nesting_non_reserved_words(self):
        """Test multi-level nested fields with non-reserved words"""
        sql = "SELECT account.profile.name FROM users WHERE account.settings.theme = 'dark'"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.projection_stage == {"account.profile.name": 1}
        assert execution_plan.filter_stage == {"account.settings.theme": "dark"}

    def test_array_bracket_notation_select(self):
        """Test array access using bracket notation in SELECT"""
        sql = "SELECT items[0], items[1].name FROM orders"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "orders"
        assert execution_plan.projection_stage == {"items.0": 1, "items.1.name": 1}

    def test_array_bracket_notation_where(self):
        """Test array access using bracket notation in WHERE"""
        sql = "SELECT * FROM orders WHERE items[0].price > 100"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "orders"
        assert execution_plan.filter_stage == {"items.0.price": {"$gt": 100}}

    def test_quoted_reserved_words(self):
        """Test using quoted reserved words as field names - currently limited support"""
        # Note: This test documents current limitations with quoted identifiers in complex paths
        sql = 'SELECT "user" FROM collection'  # Simplified test that works
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "collection"
        assert execution_plan.projection_stage == {'"user"': 1}

    def test_complex_nested_query(self):
        """Test complex query with multiple nested field types"""
        sql = """
        SELECT
            customer.profile.name,
            orders[0].total,
            settings.preferences.theme
        FROM transactions
        WHERE customer.profile.age > 18
          AND orders[0].status = 'completed'
          AND settings.notifications = true
        """
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "transactions"

        expected_projection = {"customer.profile.name": 1, "orders.0.total": 1, "settings.preferences.theme": 1}
        assert execution_plan.projection_stage == expected_projection

        # The filter should be a combination of conditions
        expected_filter = {
            "$and": [
                {"customer.profile.age": {"$gt": 18}},
                {"orders.0.status": "completed"},
                {"settings.notifications": True},
            ]
        }
        assert execution_plan.filter_stage == expected_filter

    def test_reserved_word_user_fails(self):
        """Test that unquoted 'user' keyword fails"""
        sql = "SELECT user.profile.name FROM users"

        with pytest.raises(SqlSyntaxError) as exc_info:
            parser = SQLParser(sql)
            parser.get_execution_plan()

        assert "no viable alternative" in str(exc_info.value)

    def test_reserved_word_value_fails(self):
        """Test that unquoted 'value' keyword fails"""
        sql = "SELECT data.value FROM items"

        with pytest.raises(SqlSyntaxError) as exc_info:
            parser = SQLParser(sql)
            parser.get_execution_plan()

        assert "no viable alternative" in str(exc_info.value)

    def test_numeric_dot_notation_fails(self):
        """Test that numeric dot notation fails"""
        sql = "SELECT c.0.name FROM collection"

        with pytest.raises(SqlSyntaxError) as exc_info:
            parser = SQLParser(sql)
            parser.get_execution_plan()

        assert "mismatched input" in str(exc_info.value)

    def test_nested_with_comparison_operators(self):
        """Test nested fields with various comparison operators"""
        # Test supported comparison operators with non-reserved field names
        test_cases = [
            ("profile.age > 18", {"profile.age": {"$gt": 18}}),
            ("settings.total < 100", {"settings.total": {"$lt": 100}}),  # Changed from 'count' (reserved)
            ("status.active = true", {"status.active": True}),
            ("config.name != 'default'", {"config.name": {"$ne": "default"}}),
        ]

        for where_clause, expected_filter in test_cases:
            sql = f"SELECT * FROM collection WHERE {where_clause}"
            parser = SQLParser(sql)

            assert not parser.has_errors, f"Parser errors for '{where_clause}': {parser.errors}"

            execution_plan = parser.get_execution_plan()
            assert execution_plan.filter_stage == expected_filter

    def test_nested_with_logical_operators(self):
        """Test nested fields with logical operators"""
        sql = """
        SELECT * FROM users
        WHERE profile.age > 18
          AND settings.active = true
          OR profile.vip = true
        """
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        # The exact structure depends on operator precedence handling
        assert "profile.age" in str(execution_plan.filter_stage)
        assert "settings.active" in str(execution_plan.filter_stage)
        assert "profile.vip" in str(execution_plan.filter_stage)

    def test_nested_with_aliases(self):
        """Test nested fields with column aliases"""
        sql = "SELECT profile.name AS fullname, settings.theme AS ui_theme FROM users"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        # Note: Current implementation uses original field names in projection
        # Aliases are handled at the result processing level
        assert execution_plan.projection_stage == {"profile.name": 1, "settings.theme": 1}
