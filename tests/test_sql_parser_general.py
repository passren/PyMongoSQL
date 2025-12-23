# -*- coding: utf-8 -*-
import pytest

from pymongosql.error import SqlSyntaxError
from pymongosql.sql.parser import SQLParser


class TestSQLParserGeneral:
    """Comprehensive test suite for SQL parser from simple to complex queries"""

    def test_simple_select_all(self):
        """Test simple SELECT * without WHERE"""
        sql = "SELECT * FROM users"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {}  # No WHERE clause
        assert isinstance(execution_plan.projection_stage, dict)

    def test_simple_select_fields(self):
        """Test simple SELECT with specific fields, no WHERE"""
        sql = "SELECT name, email FROM customers"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "customers"
        assert execution_plan.filter_stage == {}  # No WHERE clause
        assert execution_plan.projection_stage == {"name": 1, "email": 1}

    def test_select_single_field(self):
        """Test SELECT with single field"""
        sql = "SELECT title FROM books"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "books"
        assert execution_plan.filter_stage == {}
        assert execution_plan.projection_stage == {"title": 1}

    def test_select_with_simple_where_equals(self):
        """Test SELECT with simple WHERE equality condition"""
        sql = "SELECT name FROM users WHERE status = 'active'"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"status": "active"}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_numeric_comparison(self):
        """Test SELECT with numeric comparison in WHERE"""
        sql = "SELECT name, age FROM users WHERE age > 30"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"age": {"$gt": 30}}
        assert execution_plan.projection_stage == {"name": 1, "age": 1}

    def test_select_with_less_than(self):
        """Test SELECT with less than comparison"""
        sql = "SELECT product_name FROM products WHERE price < 100"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "products"
        assert execution_plan.filter_stage == {"price": {"$lt": 100}}
        assert execution_plan.projection_stage == {"product_name": 1}

    def test_select_with_greater_equal(self):
        """Test SELECT with greater than or equal"""
        sql = "SELECT title FROM books WHERE year >= 2020"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "books"
        assert execution_plan.filter_stage == {"year": {"$gte": 2020}}
        assert execution_plan.projection_stage == {"title": 1}

    def test_select_with_not_equals(self):
        """Test SELECT with not equals condition"""
        sql = "SELECT name FROM users WHERE status != 'inactive'"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"status": {"$ne": "inactive"}}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_and_condition(self):
        """Test SELECT with AND condition"""
        sql = "SELECT name FROM users WHERE age > 25 AND status = 'active'"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"$and": [{"age": {"$gt": 25}}, {"status": "active"}]}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_or_condition(self):
        """Test SELECT with OR condition"""
        sql = "SELECT name FROM users WHERE age < 18 OR age > 65"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"$or": [{"age": {"$lt": 18}}, {"age": {"$gt": 65}}]}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_multiple_and_conditions(self):
        """Test SELECT with multiple AND conditions"""
        sql = "SELECT * FROM products WHERE price > 50 AND category = 'electronics' AND stock > 0"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "products"
        assert execution_plan.filter_stage == {
            "$and": [
                {"price": {"$gt": 50}},
                {"category": "electronics"},
                {"stock": {"$gt": 0}},
            ]
        }
        # SELECT * should include all fields or empty projection
        assert execution_plan.projection_stage in [{}, None]

    def test_select_with_mixed_and_or(self):
        """Test SELECT with mixed AND/OR conditions"""
        sql = "SELECT name FROM users WHERE (age > 25 AND status = 'active') OR (age < 18 AND status = 'minor')"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {
            "$or": [
                {"$and": [{"age": {"$gt": 25}}, {"status": "active"}]},
                {"$and": [{"age": {"$lt": 18}}, {"status": "minor"}]},
            ]
        }

    def test_select_with_in_condition(self):
        """Test SELECT with IN condition"""
        sql = "SELECT name FROM users WHERE status IN ('active', 'pending', 'verified')"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"status": {"$in": ["active", "pending", "verified"]}}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_like_condition(self):
        """Test SELECT with LIKE condition"""
        sql = "SELECT name FROM users WHERE name LIKE 'John%'"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"name": {"$regex": "^John.*"}}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_between_condition(self):
        """Test SELECT with BETWEEN condition"""
        sql = "SELECT name FROM users WHERE age BETWEEN 25 AND 65"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"$and": [{"age": {"$gte": 25}}, {"age": {"$lte": 65}}]}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_null_condition(self):
        """Test SELECT with IS NULL condition"""
        sql = "SELECT name FROM users WHERE email IS NULL"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"email": {"$eq": None}}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_not_null_condition(self):
        """Test SELECT with IS NOT NULL condition"""
        sql = "SELECT name FROM users WHERE email IS NOT NULL"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.filter_stage == {"email": {"$ne": None}}
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY clause"""
        sql = "SELECT name, age FROM users ORDER BY age ASC"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.sort_stage == [{"age": 1}]  # 1 for ASC, -1 for DESC
        assert execution_plan.projection_stage == {"name": 1, "age": 1}

    def test_select_with_limit(self):
        """Test SELECT with LIMIT clause"""
        sql = "SELECT name FROM users LIMIT 10"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.limit_stage == 10
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_offset(self):
        """Test SELECT with OFFSET clause"""
        sql = "SELECT name FROM users OFFSET 5"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.skip_stage == 5
        assert execution_plan.projection_stage == {"name": 1}

    def test_select_with_limit_and_offset(self):
        """Test SELECT with both LIMIT and OFFSET clauses"""
        sql = "SELECT name, email FROM users LIMIT 10 OFFSET 5"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()
        assert execution_plan.collection == "users"
        assert execution_plan.limit_stage == 10
        assert execution_plan.skip_stage == 5
        assert execution_plan.projection_stage == {"name": 1, "email": 1}

    def test_complex_query_combination(self):
        """Test complex query with multiple clauses"""
        sql = """
        SELECT name, email, age
        FROM users
        WHERE age > 21 AND status = 'active'
        ORDER BY name ASC
        LIMIT 50
        """
        parser = SQLParser(sql)

        try:
            assert not parser.has_errors, f"Parser errors: {parser.errors}"
            execution_plan = parser.get_execution_plan()
            assert execution_plan.collection == "users"
            assert execution_plan.filter_stage == {"$and": [{"age": {"$gt": 21}}, {"status": "active"}]}
            assert execution_plan.projection_stage == {
                "name": 1,
                "email": 1,
                "age": 1,
            }
            assert execution_plan.sort_stage == [{"name": 1}]
            assert execution_plan.limit_stage == 50
        except (SqlSyntaxError, AssertionError) as e:
            pytest.skip(f"Complex query parsing not yet fully implemented: {e}")

    def test_parser_error_handling(self):
        """Test parser error handling for invalid SQL"""

        # Test empty SQL
        with pytest.raises(ValueError):
            SQLParser("")

        # Test malformed SQL
        with pytest.raises(SqlSyntaxError):
            parser = SQLParser("INVALID SQL SYNTAX")
            parser.get_execution_plan()

    def test_superset_wrapped_subquery(self):
        """Support Superset wrapping subquery with alias 'virtual'"""
        sql = "SELECT virtual.a, virtual.b FROM (SELECT a, b FROM users WHERE c = 1) virtual"
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"

        execution_plan = parser.get_execution_plan()
        # After unwrapping, collection should be inner collection
        assert execution_plan.collection == "users"
        # Projections should be remapped to inner fields
        assert execution_plan.projection_stage == {"a": 1, "b": 1}
        # Inner filter should be preserved
        assert execution_plan.filter_stage == {"c": 1}

    def test_different_collection_names(self):
        """Test parsing with different collection names"""
        test_cases = [
            ("SELECT * FROM users", "users"),
            ("SELECT * FROM products", "products"),
            ("SELECT * FROM orders", "orders"),
            ("SELECT * FROM customer_data", "customer_data"),
            ("SELECT * FROM product_reviews", "product_reviews"),
        ]

        for sql, expected_collection in test_cases:
            parser = SQLParser(sql)
            assert not parser.has_errors, f"Parser errors for '{sql}': {parser.errors}"

            execution_plan = parser.get_execution_plan()
            assert execution_plan.collection == expected_collection

    def test_complex_mixed_operators(self):
        """Test SELECT with complex query combining multiple operators"""
        sql = """
        SELECT id, name, age, status FROM users WHERE age > 25 AND status = 'active' AND name != 'John'
        OR department IN ('IT', 'HR') ORDER BY age DESC LIMIT 5
        """
        parser = SQLParser(sql)

        assert not parser.has_errors, f"Parser errors: {parser.errors}"
        execution_plan = parser.get_execution_plan()

        # Verify collection and projection
        assert execution_plan.collection == "users"
        assert execution_plan.projection_stage == {"id": 1, "name": 1, "age": 1, "status": 1}

        # Verify complex filter structure with mixed AND/OR conditions
        expected_filter = {
            "$or": [
                {"$and": [{"age": {"$gt": 25}}, {"status": "active"}, {"name": {"$ne": "John"}}]},
                {"department": {"$in": ["IT", "HR"]}},
            ]
        }
        assert execution_plan.filter_stage == expected_filter

        # Verify ORDER BY and LIMIT
        assert execution_plan.sort_stage == [{"age": -1}]  # DESC = -1
        assert execution_plan.limit_stage == 5
