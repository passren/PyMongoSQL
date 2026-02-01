# -*- coding: utf-8 -*-
from pymongosql.executor import ExecutionContext, ExecutionPlanFactory
from pymongosql.helper import ConnectionHelper
from pymongosql.superset_mongodb.executor import SupersetExecution


class TestSupersetConnectionString:
    """Test parsing of superset connection strings"""

    def test_parse_superset_mode_with_query_param(self):
        """Test parsing connection string with ?mode=superset query parameter"""
        mode, db, normalized = ConnectionHelper.parse_connection_string(
            "mongodb://localhost:27017/testdb?mode=superset"
        )
        assert mode == "superset"
        assert db == "testdb"
        assert "mode" not in normalized
        assert normalized == "mongodb://localhost:27017/testdb"

    def test_parse_srv_with_superset_mode(self):
        """Test parsing mongodb+srv with superset mode"""
        mode, db, normalized = ConnectionHelper.parse_connection_string("mongodb+srv://localhost/testdb?mode=superset")
        assert mode == "superset"
        assert db == "testdb"
        assert "mongodb+srv" in normalized
        assert "mode" not in normalized

    def test_parse_core_mode(self):
        """Test parsing standard mongodb:// connection string"""
        mode, db, normalized = ConnectionHelper.parse_connection_string("mongodb://localhost:27017/testdb")
        assert mode == "standard"
        assert db == "testdb"
        assert normalized == "mongodb://localhost:27017/testdb"

    def test_parse_with_credentials(self):
        """Test parsing connection string with username and password"""
        mode, db, normalized = ConnectionHelper.parse_connection_string(
            "mongodb://user:pass@localhost:27017/testdb?mode=superset"
        )
        assert mode == "superset"
        assert db == "testdb"
        assert "user:pass@localhost" in normalized
        assert "mode" not in normalized

    def test_parse_with_query_params(self):
        """Test parsing connection string with multiple query parameters"""
        mode, db, normalized = ConnectionHelper.parse_connection_string(
            "mongodb://localhost:27017/testdb?mode=superset&retryWrites=true&w=majority"
        )
        assert mode == "superset"
        assert db == "testdb"
        assert "retryWrites=true" in normalized
        assert "w=majority" in normalized
        assert "mode" not in normalized

    def test_parse_none_connection_string(self):
        """Test parsing None connection string returns defaults"""
        mode, db, normalized = ConnectionHelper.parse_connection_string(None)
        assert mode == "standard"
        assert db is None
        assert normalized is None

    def test_parse_empty_connection_string(self):
        """Test parsing empty connection string returns defaults"""
        mode, db, normalized = ConnectionHelper.parse_connection_string("")
        assert mode == "standard"
        assert db is None
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
        """Test that StandardQueryExecution doesn't support subqueries"""
        from pymongosql.executor import StandardQueryExecution

        subquery_sql = "SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id > 10"
        context = ExecutionContext(subquery_sql, "superset")

        standard_strategy = StandardQueryExecution()
        assert standard_strategy.supports(context) is False

    def test_get_strategy_selects_subquery_execution(self):
        """Test that get_strategy returns SupersetExecution for subquery context"""
        subquery_sql = "SELECT * FROM (SELECT id, name FROM users) AS u WHERE u.id > 10"
        context = ExecutionContext(subquery_sql, "superset")

        strategy = ExecutionPlanFactory.get_strategy(context)
        assert isinstance(strategy, SupersetExecution)

    def test_get_strategy_selects_standard_execution(self):
        """Test that get_strategy returns StandardQueryExecution for simple queries"""
        from pymongosql.executor import StandardQueryExecution

        simple_sql = "SELECT id, name FROM users WHERE id > 10"
        context = ExecutionContext(simple_sql)

        strategy = ExecutionPlanFactory.get_strategy(context)
        assert isinstance(strategy, StandardQueryExecution)


class TestConnectionModeDetection:
    """Test connection mode detection in Connection class"""

    def test_superset_mode_detection(self):
        """Test that superset mode is correctly detected"""
        from pymongosql.helper import ConnectionHelper

        mode, db, _ = ConnectionHelper.parse_connection_string("mongodb://localhost:27017/testdb?mode=superset")
        assert mode == "superset"

    def test_core_mode_detection(self):
        """Test that core mode is correctly detected"""
        from pymongosql.helper import ConnectionHelper

        mode, db, _ = ConnectionHelper.parse_connection_string("mongodb://localhost:27017/testdb")
        assert mode == "standard"


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
        """Test simple subquery wrapping on users (Superset-style SQL)"""
        assert superset_conn.mode == "superset"

        cursor = superset_conn.cursor()

        # Superset-style query with column aliases
        subquery_sql = """
        SELECT _id AS _id, name AS name, age AS age
        FROM (SELECT _id, name, age FROM users) AS virtual_table
        LIMIT 5
        """

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        assert len(rows) == 5

        # Verify column names
        description = cursor.description
        col_names = [desc[0] for desc in description] if description else []
        assert "_id" in col_names
        assert "name" in col_names
        assert "age" in col_names

    def test_subquery_with_where_condition(self, superset_conn):
        """Test subquery with WHERE on wrapper (Superset-style SQL)"""
        cursor = superset_conn.cursor()

        # Superset-style query with column aliases and WHERE clause
        subquery_sql = """
        SELECT _id AS _id, name AS name, age AS age
        FROM (SELECT _id, name, age FROM users) AS virtual_table
        WHERE age > 30
        """

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        # Should have results where age > 30
        assert len(rows) == 11

    def test_subquery_products_by_price_range(self, superset_conn):
        """Test subquery filtering products by price range (Superset-style SQL)"""
        cursor = superset_conn.cursor()

        # Superset-style query with column aliases and GROUP BY
        subquery_sql = """
        SELECT _id AS _id, name AS name, price AS price, category AS category
        FROM (SELECT _id, name, price, category FROM products WHERE price > 100) AS virtual_table
        WHERE price < 2000
        GROUP BY _id, name, price, category
        LIMIT 10
        """

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        assert len(rows) == 10

    def test_subquery_orders_aggregation(self, superset_conn):
        """Test subquery on orders with multiple conditions (Superset-style SQL)"""
        cursor = superset_conn.cursor()

        # Superset-style query with column aliases and GROUP BY aggregation
        subquery_sql = """
        SELECT order_date AS order_date, status AS status, total_amount AS total_amount, currency AS currency
        FROM (SELECT order_date, status, total_amount, currency FROM orders) AS virtual_table
        GROUP BY order_date, status, total_amount, currency
        LIMIT 18
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

    def test_description_matches_data_length(self, superset_conn):
        """Test that cursor.description column count matches actual data tuple length"""
        cursor = superset_conn.cursor()

        # Superset-style query with column aliases
        subquery_sql = """
        SELECT _id AS _id, name AS name, age AS age
        FROM (SELECT _id, name, age FROM users) AS virtual_table
        LIMIT 5
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        # Verify description exists
        assert description is not None
        assert len(description) > 0

        # Verify each row tuple has same length as description
        for row in rows:
            assert len(row) == len(
                description
            ), f"Row tuple length {len(row)} doesn't match description length {len(description)}"

    def test_description_column_names_match_data(self, superset_conn):
        """Test that description column names match the actual data fields"""
        cursor = superset_conn.cursor()

        # Superset-style query with column aliases and GROUP BY
        subquery_sql = """
        SELECT _id AS _id, name AS name, age AS age
        FROM (SELECT _id, name, age FROM users) AS virtual_table
        GROUP BY _id, name, age
        LIMIT 3
        """
        cursor.execute(subquery_sql)
        _ = cursor.fetchall()
        description = cursor.description

        # Extract column names from description
        col_names = [desc[0] for desc in description]

        # Verify expected columns are present
        assert "_id" in col_names
        assert "name" in col_names
        assert "age" in col_names

        # Verify description has correct structure (7-tuple per DB API 2.0)
        for desc in description:
            assert len(desc) == 7  # name, type_code, display_size, internal_size, precision, scale, null_ok

    def test_data_values_integrity_through_stages(self, superset_conn):
        """Test that data values are preserved correctly through MongoDB->SQLite->ResultSet stages"""
        cursor = superset_conn.cursor()

        # First, get a known user name from the database
        cursor.execute("SELECT _id AS _id, name AS name FROM (SELECT _id, name FROM users) AS virtual_table LIMIT 1")
        sample_rows = cursor.fetchall()
        assert len(sample_rows) >= 1
        known_name = sample_rows[0][1]

        # Now query for that specific user with Superset-style SQL
        cursor.execute(
            f"SELECT _id AS _id, name AS name FROM (SELECT _id, name FROM users) AS virtual_table "
            f"WHERE name = '{known_name}' GROUP BY _id, name LIMIT 1"
        )
        rows = cursor.fetchall()

        # Verify data was retrieved
        assert len(rows) >= 1

        # Verify data structure
        row = rows[0]
        assert len(row) == 2  # _id and name
        assert row[1] == known_name  # Name should match the known value

    def test_description_consistency_across_fetches(self, superset_conn):
        """Test that cursor.description remains consistent across multiple fetches"""
        cursor = superset_conn.cursor()

        # Superset-style query with column aliases and GROUP BY
        subquery_sql = """
        SELECT _id AS _id, name AS name, price AS price
        FROM (SELECT _id, name, price FROM products) AS virtual_table
        GROUP BY _id, name, price
        LIMIT 10
        """
        cursor.execute(subquery_sql)

        # Get description before fetch
        description_before = cursor.description
        assert description_before is not None

        # Fetch all and get description again
        _ = cursor.fetchall()
        description_after = cursor.description

        # Descriptions should be identical
        assert description_before == description_after
        assert len(description_before) == 3  # _id, name, price

    def test_all_columns_in_description_match_data(self, superset_conn):
        """Test that all columns in description are present in actual data"""
        cursor = superset_conn.cursor()

        # Superset-style query with column aliases and GROUP BY aggregation
        subquery_sql = """
        SELECT _id AS _id, order_date AS order_date, status AS status, total_amount AS total_amount
        FROM (SELECT _id, order_date, status, total_amount FROM orders) AS virtual_table
        GROUP BY _id, order_date, status, total_amount
        LIMIT 5
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        assert len(rows) > 0
        assert description is not None

        # Verify description has 4 columns
        assert len(description) == 4

        # Extract column names from description
        desc_col_names = [desc[0] for desc in description]

        # Verify expected columns
        expected_cols = ["_id", "order_date", "status", "total_amount"]
        for expected_col in expected_cols:
            assert expected_col in desc_col_names, f"Expected column {expected_col} not in description"

        # Verify every row has 4 values (matching description)
        for row in rows:
            assert len(row) == 4, f"Row has {len(row)} values but description has {len(description)} columns"

    def test_projection_functions_with_superset_execution(self, superset_conn):
        """Test that projection functions work correctly in superset two-stage execution"""
        cursor = superset_conn.cursor()

        # Query using projection functions in INNER query (MongoDB execution)
        # Projection functions are applied during MongoDB stage, results flow to SQLite
        subquery_sql = """
        SELECT id, numeric_age, creation_date, last_updated
        FROM (SELECT _id AS id,
                     NUMBER(age) AS numeric_age,
                     DATE(created_at) AS creation_date,
                     DATETIME(updated_at) AS last_updated
              FROM users WHERE age > 0) AS virtual_table
        LIMIT 3
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        assert len(rows) > 0
        assert description is not None
        assert len(description) == 4

        # Extract column names and type codes
        col_info = [(desc[0], desc[1]) for desc in description]
        col_names = [name for name, _ in col_info]
        type_codes = {name: type_code for name, type_code in col_info}

        # Verify column names
        assert "id" in col_names
        assert "numeric_age" in col_names
        assert "creation_date" in col_names
        assert "last_updated" in col_names

        # Verify type codes match projection functions
        # All projection functions return string type codes like 'float', 'datetime', 'date', etc.
        import datetime

        # NUMBER() should produce 'float' type code
        assert type_codes["numeric_age"] == "float", f"Expected 'float' but got {type_codes['numeric_age']!r}"

        # DATE() should produce 'datetime' type code (note: projection functions return 'datetime' for both DATE and DATETIME)
        assert type_codes["creation_date"] == "datetime", f"Expected 'datetime' but got {type_codes['creation_date']!r}"

        # DATETIME() should produce 'datetime' type code
        assert type_codes["last_updated"] == "datetime", f"Expected 'datetime' but got {type_codes['last_updated']!r}"

        # Verify data values are correctly converted
        for row in rows:
            numeric_age_idx = col_names.index("numeric_age")
            creation_date_idx = col_names.index("creation_date")
            last_updated_idx = col_names.index("last_updated")

            # NUMBER() should convert to float
            assert isinstance(
                row[numeric_age_idx], (int, float)
            ), f"numeric_age should be numeric, got {type(row[numeric_age_idx])}"

            # DATE() should convert to date object
            creation_date_val = row[creation_date_idx]
            assert creation_date_val is None or isinstance(
                creation_date_val, datetime.date
            ), f"creation_date should be date or None, got {type(creation_date_val)}"

            # DATETIME() should convert to datetime object
            last_updated_val = row[last_updated_idx]
            assert last_updated_val is None or isinstance(
                last_updated_val, datetime.datetime
            ), f"last_updated should be datetime or None, got {type(last_updated_val)}"

    def test_projection_functions_with_custom_format(self, superset_conn):
        """Test that projection functions with custom format parameters work correctly in superset execution"""
        cursor = superset_conn.cursor()

        # Query using projection functions with custom formats in INNER query (MongoDB execution)
        # Format parameters are processed during MongoDB stage
        subquery_sql = """
        SELECT id, formatted_date, formatted_datetime, timestamp_value
        FROM (SELECT _id AS id,
                     DATE(created_at, '%Y-%m-%d') AS formatted_date,
                     DATETIME(updated_at, '%Y-%m-%d %H:%M:%S') AS formatted_datetime,
                     TIMESTAMP(created_at, '%Y-%m-%d') AS timestamp_value
              FROM users) AS virtual_table
        LIMIT 3
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        assert len(rows) > 0
        assert description is not None
        assert len(description) == 4

        # Extract column names
        col_names = [desc[0] for desc in description]

        # Verify column names
        assert "id" in col_names
        assert "formatted_date" in col_names
        assert "formatted_datetime" in col_names
        assert "timestamp_value" in col_names

        # Verify type codes from description
        type_codes = {desc[0]: desc[1] for desc in description}

        import datetime

        from bson import Timestamp

        # Verify expected type codes
        # All projection functions return string type codes
        # DATE() should produce 'datetime' type code
        assert (
            type_codes["formatted_date"] == "datetime"
        ), f"Expected 'datetime' but got {type_codes['formatted_date']!r}"

        # DATETIME() should produce 'datetime' type code
        assert (
            type_codes["formatted_datetime"] == "datetime"
        ), f"Expected 'datetime' but got {type_codes['formatted_datetime']!r}"

        # TIMESTAMP() should produce 'datetime' type code
        assert (
            type_codes["timestamp_value"] == "datetime"
        ), f"Expected 'datetime' but got {type_codes['timestamp_value']!r}"

        # Verify data values are correctly converted with custom formats
        for row in rows:
            formatted_date_idx = col_names.index("formatted_date")
            formatted_datetime_idx = col_names.index("formatted_datetime")
            timestamp_value_idx = col_names.index("timestamp_value")

            # DATE() with format should convert to date object
            date_value = row[formatted_date_idx]
            assert date_value is None or isinstance(
                date_value, datetime.date
            ), f"formatted_date should be date or None, got {type(date_value)}"

            # DATETIME() with format should convert to datetime object
            datetime_value = row[formatted_datetime_idx]
            assert datetime_value is None or isinstance(
                datetime_value, datetime.datetime
            ), f"formatted_datetime should be datetime or None, got {type(datetime_value)}"

            # TIMESTAMP() with format should convert to Timestamp object
            timestamp_value = row[timestamp_value_idx]
            assert timestamp_value is None or isinstance(
                timestamp_value, Timestamp
            ), f"timestamp_value should be Timestamp or None, got {type(timestamp_value)}"

    def test_empty_result_with_valid_description(self, superset_conn):
        """Test that description is available for result sets, even if empty after filtering"""
        cursor = superset_conn.cursor()

        # Superset-style query that filters to empty results using a numeric condition
        # Use a very large age value that unlikely to exist
        subquery_sql = """
        SELECT _id AS _id, name AS name, age AS age
        FROM (SELECT _id, name, age FROM users) AS virtual_table
        WHERE age > 999
        GROUP BY _id, name, age
        """
        cursor.execute(subquery_sql)
        _ = cursor.fetchall()
        description = cursor.description

        # Description should be available based on projection_stage
        # (even if actual data is empty, the schema is known)
        assert description is not None
        assert len(description) == 3
        col_names = [desc[0] for desc in description]
        assert "_id" in col_names
        assert "name" in col_names
        assert "age" in col_names

    def test_subquery_with_field_aliases_in_inner_query(self, superset_conn):
        """Test superset execution with field aliases in the inner MongoDB query"""
        cursor = superset_conn.cursor()

        # Inner query has aliases
        subquery_sql = "SELECT * FROM (SELECT name AS user_name, email AS user_email FROM users) AS u LIMIT 3"

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()

        # Check that aliases appear in cursor description
        assert cursor.result_set.description is not None
        col_names = [desc[0] for desc in cursor.result_set.description]

        # Aliases from inner query should be preserved
        assert "user_name" in col_names
        assert "user_email" in col_names
        assert len(rows) <= 3

    def test_subquery_with_nested_field_alias(self, superset_conn):
        """Test superset execution with nested field alias in inner query"""
        cursor = superset_conn.cursor()

        # Inner query has nested field with alias
        subquery_sql = "SELECT * FROM (SELECT _id AS id, name FROM users) AS u WHERE u.id > 0 LIMIT 2"

        cursor.execute(subquery_sql)
        rows = cursor.fetchall()

        # Check that aliases appear in cursor description
        assert cursor.result_set.description is not None
        col_names = [desc[0] for desc in cursor.result_set.description]

        # Alias from inner query should be preserved
        assert "id" in col_names
        assert len(rows) <= 2

    def test_where_clause_functions_with_superset_execution(self, superset_conn):
        """Test that WHERE clause functions work correctly in superset two-stage execution"""
        cursor = superset_conn.cursor()

        # Query using WHERE clause functions in the outer query (SQLite stage)
        # The WHERE function is applied to filter results after MongoDB stage
        subquery_sql = """
        SELECT _id AS _id, name AS name, created_at AS created_at
        FROM (SELECT _id, name, created_at FROM users) AS virtual_table
        WHERE created_at > date('2023-01-01')
        LIMIT 5
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        assert len(rows) > 0, "Should have rows with created_at > 2023-01-01"
        assert description is not None
        assert len(description) == 3

        # Extract column names
        col_names = [desc[0] for desc in description]
        assert "_id" in col_names
        assert "name" in col_names
        assert "created_at" in col_names

    def test_where_clause_datetime_function_with_superset_execution(self, superset_conn):
        """Test WHERE clause DATETIME() function in superset execution"""
        cursor = superset_conn.cursor()

        # Query using DATETIME() function in WHERE clause with created_at field
        subquery_sql = """
        SELECT _id AS _id, name AS name, created_at AS created_at
        FROM (SELECT _id, name, created_at FROM users) AS virtual_table
        WHERE created_at > datetime('2020-01-01 00:00:00')
        LIMIT 5
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        assert len(rows) > 0, "Should have rows with created_at > 2020-01-01"
        assert description is not None
        assert len(description) == 3

        # Extract column names
        col_names = [desc[0] for desc in description]
        assert "_id" in col_names
        assert "name" in col_names
        assert "created_at" in col_names

    def test_where_clause_between_with_date_function(self, superset_conn):
        """Test WHERE clause with BETWEEN using DATE() function in superset execution"""
        cursor = superset_conn.cursor()

        # Query using DATE() function in BETWEEN clause
        subquery_sql = """
        SELECT _id AS _id, name AS name, created_at AS created_at
        FROM (SELECT _id, name, created_at FROM users) AS virtual_table
        WHERE created_at BETWEEN date('2000-01-01') AND date('2030-12-31')
        LIMIT 5
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        assert len(rows) > 0, "Should have rows with created_at in range"
        assert description is not None
        assert len(description) == 3

        # Extract column names
        col_names = [desc[0] for desc in description]
        assert "_id" in col_names
        assert "name" in col_names
        assert "created_at" in col_names

    def test_where_clause_function_with_multiple_conditions(self, superset_conn):
        """Test WHERE clause functions combined with other conditions in superset execution"""
        cursor = superset_conn.cursor()

        # Query using WHERE function with additional AND conditions
        # Both conditions are in the outer query WHERE clause
        subquery_sql = """
        SELECT _id AS _id, name AS name, age AS age, created_at AS created_at
        FROM (SELECT _id, name, age, created_at FROM users) AS virtual_table
        WHERE age > 25 AND created_at > date('2023-01-01')
        GROUP BY _id, name, age, created_at
        LIMIT 5
        """
        cursor.execute(subquery_sql)
        rows = cursor.fetchall()
        description = cursor.description

        assert len(rows) > 0, "Should have rows matching both age > 25 and created_at > 2023-01-01"
        assert description is not None
        assert len(description) == 4

        # Extract column names
        col_names = [desc[0] for desc in description]
        assert "_id" in col_names
        assert "name" in col_names
        assert "age" in col_names
        assert "created_at" in col_names


class TestSubqueryDetector:
    """Test subquery detection and outer query extraction"""

    def test_detect_wrapped_subquery(self):
        """Test detection of wrapped subquery pattern"""
        from pymongosql.superset_mongodb.detector import SubqueryDetector

        query = "SELECT col1, col2 FROM (SELECT col1, col2 FROM table1) AS t1 WHERE col1 > 5"
        info = SubqueryDetector.detect(query)

        assert info.has_subquery is True
        assert info.is_wrapped is True
        assert info.subquery_alias == "t1"

    def test_extract_outer_query_preserves_select_clause(self):
        """Test that extract_outer_query preserves SELECT clause with column aliases"""
        from pymongosql.superset_mongodb.detector import SubqueryDetector

        # Exact pattern from Superset
        query = """
        SELECT order_date AS order_date, status AS status, total_amount AS total_amount, currency AS currency
        FROM (SELECT order_date, status, total_amount, currency FROM orders) AS virtual_table
        GROUP BY order_date, status, total_amount, currency
        LIMIT 1000
        """

        result = SubqueryDetector.extract_outer_query(query)
        assert result is not None

        outer_query, table_alias = result

        # Verify table alias is correct
        assert table_alias == "virtual_table"

        # Verify SELECT clause is preserved
        assert "order_date AS order_date" in outer_query
        assert "status AS status" in outer_query
        assert "total_amount AS total_amount" in outer_query
        assert "currency AS currency" in outer_query

        # Verify it has the table reference
        assert "virtual_table" in outer_query

        # Verify GROUP BY is preserved
        assert "GROUP BY" in outer_query
        assert "order_date" in outer_query
        assert "status" in outer_query

        # Verify LIMIT is preserved
        assert "LIMIT 1000" in outer_query

    def test_extract_outer_query_with_where_clause(self):
        """Test outer query extraction with WHERE clause"""
        from pymongosql.superset_mongodb.detector import SubqueryDetector

        query = """
        SELECT _id AS _id, name AS name
        FROM (SELECT _id, name FROM users) AS virtual_table
        WHERE name = 'test'
        GROUP BY _id, name
        """

        result = SubqueryDetector.extract_outer_query(query)
        assert result is not None

        outer_query, table_alias = result

        # Verify WHERE clause is preserved
        assert "WHERE name = 'test'" in outer_query
        assert "_id AS _id" in outer_query
        assert "virtual_table" in outer_query

    def test_extract_outer_query_complex_pattern(self):
        """Test extraction with complex column aliases and multiple conditions"""
        from pymongosql.superset_mongodb.detector import SubqueryDetector

        query = """
        SELECT col1 AS column_one, col2 AS column_two, col3 AS column_three
        FROM (SELECT col1, col2, col3 FROM data_table WHERE col1 > 0) AS virtual_table
        WHERE col2 IS NOT NULL
        GROUP BY col1, col2, col3
        ORDER BY col1 DESC
        LIMIT 100
        """

        result = SubqueryDetector.extract_outer_query(query)
        assert result is not None

        outer_query, table_alias = result

        # Verify all important elements are preserved
        assert "col1 AS column_one" in outer_query
        assert "col2 AS column_two" in outer_query
        assert "col3 AS column_three" in outer_query
        assert "WHERE col2 IS NOT NULL" in outer_query
        assert "GROUP BY col1, col2, col3" in outer_query
        assert "ORDER BY col1 DESC" in outer_query
        assert "LIMIT 100" in outer_query
        assert "virtual_table" in outer_query
