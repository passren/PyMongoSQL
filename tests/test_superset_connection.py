# -*- coding: utf-8 -*-
"""
Tests for superset subquery connection mode.

Tests the mongodb+superset:// connection pattern and verifies that:
1. Superset mode is correctly detected from connection strings
2. SubqueryExecution strategy is registered and used
3. Subqueries are supported in superset mode
4. Subqueries are rejected in core mode
"""


from pymongosql.executor import ExecutionContext, ExecutionPlanFactory
from pymongosql.helper import ConnectionHelper
from pymongosql.superset_mongodb.executor import SupersetExecution


class TestSupersetConnectionString:
    """Test parsing of superset connection strings"""

    def test_parse_superset_mode(self):
        """Test parsing mongodb+superset:// connection string"""
        mode, normalized = ConnectionHelper.parse_connection_string("mongodb+superset://localhost:27017/testdb")
        assert mode == "superset"
        assert normalized == "mongodb://localhost:27017/testdb"

    def test_parse_core_mode(self):
        """Test parsing standard mongodb:// connection string"""
        mode, normalized = ConnectionHelper.parse_connection_string("mongodb://localhost:27017/testdb")
        assert mode == "standard"
        assert normalized == "mongodb://localhost:27017/testdb"

    def test_parse_with_credentials(self):
        """Test parsing connection string with username and password"""
        mode, normalized = ConnectionHelper.parse_connection_string(
            "mongodb+superset://user:pass@localhost:27017/testdb"
        )
        assert mode == "superset"
        assert "user:pass@localhost" in normalized

    def test_parse_with_query_params(self):
        """Test parsing connection string with query parameters"""
        mode, normalized = ConnectionHelper.parse_connection_string(
            "mongodb+superset://localhost:27017/testdb?retryWrites=true&w=majority"
        )
        assert mode == "superset"
        assert "retryWrites=true" in normalized
        assert "w=majority" in normalized

    def test_parse_none_connection_string(self):
        """Test parsing None connection string returns defaults"""
        mode, normalized = ConnectionHelper.parse_connection_string(None)
        assert mode == "standard"
        assert normalized is None

    def test_parse_empty_connection_string(self):
        """Test parsing empty connection string returns defaults"""
        mode, normalized = ConnectionHelper.parse_connection_string("")
        assert mode == "standard"
        assert normalized is None


class TestSupersetExecutionStrategy:
    """Test SubqueryExecution strategy registration"""

    def test_subquery_execution_registered(self):
        """Test that SupersetExecution strategy is registered"""
        strategies = ExecutionPlanFactory._strategies
        strategy_names = [s.__class__.__name__ for s in strategies]
        assert "SupersetExecution" in strategy_names

    def test_subquery_execution_supports_subqueries(self):
        """Test that SupersetExecution supports subquery contexts"""
        subquery_sql = "SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id > 10"
        context = ExecutionContext(subquery_sql, "superset")

        superset_strategy = SupersetExecution()
        assert superset_strategy.supports(context) is True

    def test_standard_execution_rejects_subqueries(self):
        """Test that StandardExecution doesn't support subqueries"""
        from pymongosql.executor import StandardExecution

        subquery_sql = "SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id > 10"
        context = ExecutionContext(subquery_sql, "superset")

        standard_strategy = StandardExecution()
        assert standard_strategy.supports(context) is False

    def test_get_strategy_selects_subquery_execution(self):
        """Test that get_strategy returns SupersetExecution for subquery context"""
        subquery_sql = "SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id > 10"
        context = ExecutionContext(subquery_sql, "superset")

        strategy = ExecutionPlanFactory.get_strategy(context)
        assert isinstance(strategy, SupersetExecution)

    def test_get_strategy_selects_standard_execution(self):
        """Test that get_strategy returns StandardExecution for simple queries"""
        from pymongosql.executor import StandardExecution

        simple_sql = "SELECT id, name FROM users WHERE id > 10"
        context = ExecutionContext(simple_sql)

        strategy = ExecutionPlanFactory.get_strategy(context)
        assert isinstance(strategy, StandardExecution)


class TestConnectionModeDetection:
    """Test connection mode detection in Connection class"""

    def test_superset_mode_detection(self):
        """Test that superset mode is correctly detected"""
        from pymongosql.helper import ConnectionHelper

        is_superset, _ = ConnectionHelper.parse_connection_string("mongodb+superset://localhost:27017/testdb")
        assert is_superset == "superset"

    def test_core_mode_detection(self):
        """Test that core mode is correctly detected"""
        from pymongosql.helper import ConnectionHelper

        is_core, _ = ConnectionHelper.parse_connection_string("mongodb://localhost:27017/testdb")
        assert is_core == "standard"


class TestSubqueryExecutionIntegration:
    """Integration tests for subquery execution with real MongoDB data"""

    def test_core_connection_with_subqueries(self, conn):
        """Test that core connection with subquery execution"""
        assert conn.mode == "standard"

        cursor = conn.cursor()
        subquery_sql = "SELECT * FROM (SELECT _id, name FROM users) AS u WHERE u.age > 25"

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        assert len(rows) == 0

    def test_core_connection_with_standard_queries(self, conn):
        """Test simple query on users collection"""
        cursor = conn.cursor()
        cursor.execute("SELECT _id, name, age FROM users WHERE age > 25")

        rows = cursor.fetchall()
        assert len(rows) > 0

        # Verify column names
        description = cursor.description
        col_names = [desc[0] for desc in description] if description else []
        assert "_id" in col_names or "id" in col_names
        assert "name" in col_names
        assert "age" in col_names

    def test_subquery_simple_wrapping(self, superset_conn):
        """Test simple subquery wrapping on users"""
        assert superset_conn.mode == "superset"

        cursor = superset_conn.cursor()

        # Simple subquery: wrap a MongoDB query result
        subquery_sql = "SELECT * FROM (SELECT _id, name, age FROM users) AS u LIMIT 5"

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        assert len(rows) == 5

    def test_subquery_with_where_condition(self, superset_conn):
        """Test subquery with WHERE on wrapper"""
        cursor = superset_conn.cursor()

        # Subquery: select from users, then filter in wrapper
        subquery_sql = "SELECT * FROM (SELECT _id, name, age FROM users) AS u WHERE age > 30"

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        # Should have results where age > 30
        assert len(rows) == 11

    def test_subquery_products_by_price_range(self, superset_conn):
        """Test subquery filtering products by price range"""
        cursor = superset_conn.cursor()

        # Subquery: get products, filter by price range in wrapper
        subquery_sql = """
        SELECT * FROM (SELECT _id, name, price, category FROM products WHERE price > 100)
        AS p WHERE price < 2000 LIMIT 10
        """

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        assert len(rows) == 10

    def test_subquery_orders_aggregation(self, superset_conn):
        """Test subquery on orders with multiple conditions"""
        cursor = superset_conn.cursor()

        # Subquery: get orders, then filter for high-value completed orders
        subquery_sql = """
        SELECT * FROM (SELECT _id, user_id, total_amount, status FROM orders)
        AS o WHERE status = 'completed' LIMIT 18
        """

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        assert len(rows) == 18

    def test_multiple_queries_in_session(self, superset_conn):
        """Test running multiple queries in single superset session"""
        cursor = superset_conn.cursor()

        # Query 1: Users
        cursor.execute("SELECT _id, name, age FROM users LIMIT 3")
        users = cursor.fetchall()
        assert len(users) == 3

        # Query 2: Orders
        cursor.execute("SELECT _id, status FROM orders LIMIT 3")
        orders = cursor.fetchall()
        assert len(orders) == 3

        # Query 3: Products
        cursor.execute("SELECT _id, name, price FROM products LIMIT 3")
        products = cursor.fetchall()
        assert len(products) == 3
