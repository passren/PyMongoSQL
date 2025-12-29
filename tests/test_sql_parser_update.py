# -*- coding: utf-8 -*-
from pymongosql.sql.parser import SQLParser
from pymongosql.sql.update_builder import UpdateExecutionPlan


class TestSQLParserUpdate:
    """Tests for UPDATE parsing via AST visitor (PartiQL-style)."""

    def test_update_simple_field(self):
        """Test UPDATE with single field update."""
        sql = "UPDATE users SET name = 'John'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "users"
        assert plan.update_fields == {"name": "John"}
        assert plan.filter_conditions == {}

    def test_update_multiple_fields(self):
        """Test UPDATE with multiple field updates."""
        sql = "UPDATE products SET price = 100, stock = 50"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "products"
        assert plan.update_fields == {"price": 100, "stock": 50}
        assert plan.filter_conditions == {}

    def test_update_with_where_clause(self):
        """Test UPDATE with WHERE clause."""
        sql = "UPDATE orders SET status = 'shipped' WHERE id = 123"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "orders"
        assert plan.update_fields == {"status": "shipped"}
        assert plan.filter_conditions == {"id": 123}

    def test_update_numeric_value(self):
        """Test UPDATE with numeric value."""
        sql = "UPDATE inventory SET quantity = 100 WHERE product_id = 5"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "inventory"
        assert plan.update_fields == {"quantity": 100}
        assert plan.filter_conditions == {"product_id": 5}

    def test_update_boolean_value(self):
        """Test UPDATE with boolean value."""
        sql = "UPDATE settings SET enabled = true WHERE setting_key = 'feature_flag'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "settings"
        assert plan.update_fields == {"enabled": True}
        assert plan.filter_conditions == {"setting_key": "feature_flag"}

    def test_update_null_value(self):
        """Test UPDATE with NULL value."""
        sql = "UPDATE cache SET expires = null WHERE session_id = 'abc123'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "cache"
        assert plan.update_fields == {"expires": None}
        assert plan.filter_conditions == {"session_id": "abc123"}

    def test_update_with_comparison_operators(self):
        """Test UPDATE with various comparison operators in WHERE."""
        sql = "UPDATE products SET discount = 0.1 WHERE price > 100"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "products"
        assert plan.update_fields == {"discount": 0.1}
        assert plan.filter_conditions == {"price": {"$gt": 100}}

    def test_update_with_and_condition(self):
        """Test UPDATE with AND condition in WHERE."""
        sql = "UPDATE items SET status = 'archived' WHERE category = 'old' AND year < 2020"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "items"
        assert plan.update_fields == {"status": "archived"}
        assert "$and" in plan.filter_conditions
        assert len(plan.filter_conditions["$and"]) == 2

    def test_update_validates_execution_plan(self):
        """Test that validation is called on the execution plan."""
        sql = "UPDATE users SET email = 'test@example.com' WHERE id = 1"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.validate() is True
        assert plan.collection == "users"

    def test_update_nested_field(self):
        """Test UPDATE with nested field path."""
        sql = "UPDATE users SET address.city = 'NYC' WHERE id = 1"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "users"
        assert "address.city" in plan.update_fields
        assert plan.update_fields["address.city"] == "NYC"

    def test_update_with_parameter_placeholder(self):
        """Test UPDATE with parameter placeholder."""
        sql = "UPDATE users SET name = '?' WHERE id = '?'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "users"
        assert plan.update_fields == {"name": "?"}
        assert plan.filter_conditions == {"id": "?"}
