# -*- coding: utf-8 -*-
from pymongosql.sql.delete_builder import DeleteExecutionPlan
from pymongosql.sql.parser import SQLParser


class TestSQLParserDelete:
    """Tests for DELETE parsing via AST visitor (PartiQL-style)."""

    def test_delete_all_documents(self):
        """Test DELETE without WHERE clause."""
        sql = "DELETE FROM users"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "users"
        assert plan.filter_conditions == {}

    def test_delete_with_simple_where(self):
        """Test DELETE with simple equality WHERE clause."""
        sql = "DELETE FROM orders WHERE status = 'cancelled'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "orders"
        assert plan.filter_conditions == {"status": "cancelled"}

    def test_delete_with_numeric_filter(self):
        """Test DELETE with numeric comparison."""
        sql = "DELETE FROM products WHERE price > 100"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "products"
        assert plan.filter_conditions == {"price": {"$gt": 100}}

    def test_delete_with_less_than(self):
        """Test DELETE with less than operator."""
        sql = "DELETE FROM sessions WHERE created_at < 1609459200"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "sessions"
        assert plan.filter_conditions == {"created_at": {"$lt": 1609459200}}

    def test_delete_with_greater_equal(self):
        """Test DELETE with >= operator."""
        sql = "DELETE FROM inventory WHERE quantity >= 1000"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "inventory"
        assert plan.filter_conditions == {"quantity": {"$gte": 1000}}

    def test_delete_with_less_equal(self):
        """Test DELETE with <= operator."""
        sql = "DELETE FROM logs WHERE severity <= 2"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "logs"
        assert plan.filter_conditions == {"severity": {"$lte": 2}}

    def test_delete_with_not_equal(self):
        """Test DELETE with != operator."""
        sql = "DELETE FROM temp WHERE valid != true"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "temp"
        assert plan.filter_conditions == {"valid": {"$ne": True}}

    def test_delete_with_qmark_parameter(self):
        """Test DELETE with qmark placeholder."""
        sql = "DELETE FROM users WHERE name = '?'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "users"
        # Parameters should be in the filter as placeholders
        assert plan.filter_conditions == {"name": "?"}

    def test_delete_with_named_parameter(self):
        """Test DELETE with named parameter placeholder."""
        sql = "DELETE FROM orders WHERE order_id = ':orderId'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "orders"
        assert plan.filter_conditions == {"order_id": ":orderId"}

    def test_delete_with_null_comparison(self):
        """Test DELETE with NULL value."""
        sql = "DELETE FROM cache WHERE expires = null"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "cache"
        assert plan.filter_conditions == {"expires": None}

    def test_delete_with_boolean_true(self):
        """Test DELETE with boolean TRUE value."""
        sql = "DELETE FROM flags WHERE active = true"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "flags"
        assert plan.filter_conditions == {"active": True}

    def test_delete_with_boolean_false(self):
        """Test DELETE with boolean FALSE value."""
        sql = "DELETE FROM flags WHERE active = false"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "flags"
        assert plan.filter_conditions == {"active": False}

    def test_delete_with_string_value(self):
        """Test DELETE with string literal."""
        sql = "DELETE FROM users WHERE username = 'john_doe'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "users"
        assert plan.filter_conditions == {"username": "john_doe"}

    def test_delete_with_negative_number(self):
        """Test DELETE with negative number."""
        sql = "DELETE FROM transactions WHERE amount = -50"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "transactions"
        assert plan.filter_conditions == {"amount": -50}

    def test_delete_with_float_value(self):
        """Test DELETE with floating point number."""
        sql = "DELETE FROM measurements WHERE temperature > 36.5"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "measurements"
        assert plan.filter_conditions == {"temperature": {"$gt": 36.5}}

    def test_delete_with_and_condition(self):
        """Test DELETE with AND condition."""
        sql = "DELETE FROM items WHERE category = 'electronics' AND price > 500"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "items"
        # AND condition creates a $and array with both conditions
        assert "$and" in plan.filter_conditions
        assert len(plan.filter_conditions["$and"]) == 2
        assert {"category": "electronics"} in plan.filter_conditions["$and"]
        assert {"price": {"$gt": 500}} in plan.filter_conditions["$and"]

    def test_delete_with_or_condition(self):
        """Test DELETE with OR condition."""
        sql = "DELETE FROM logs WHERE severity = 'ERROR' OR severity = 'CRITICAL'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "logs"
        assert "$or" in plan.filter_conditions
        assert len(plan.filter_conditions["$or"]) == 2
        assert {"severity": "ERROR"} in plan.filter_conditions["$or"]
        assert {"severity": "CRITICAL"} in plan.filter_conditions["$or"]

    def test_delete_collection_name_case_sensitive(self):
        """Test that collection names are case-sensitive."""
        sql = "DELETE FROM MyCollection WHERE id = 1"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "MyCollection"
        assert plan.filter_conditions == {"id": 1}

    def test_delete_field_name_case_sensitive(self):
        """Test that field names are case-sensitive."""
        sql = "DELETE FROM users WHERE UserID = 123"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "users"
        assert plan.filter_conditions == {"UserID": 123}

    def test_delete_validates_execution_plan(self):
        """Test that validation is called on the execution plan."""
        sql = "DELETE FROM products WHERE category = 'obsolete'"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.validate() is True
        assert plan.collection == "products"
