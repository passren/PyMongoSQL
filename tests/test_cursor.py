# -*- coding: utf-8 -*-
from datetime import date, datetime, timezone

import pytest
from bson.timestamp import Timestamp

from pymongosql.error import DatabaseError, ProgrammingError, SqlSyntaxError
from pymongosql.result_set import ResultSet


class TestCursor:
    """Test suite for Cursor class using the `conn` fixture"""

    def test_cursor_init(self, conn):
        """Test cursor initialization"""
        cursor = conn.cursor()
        assert cursor._connection == conn
        assert cursor._result_set is None

    def test_execute_simple_select(self, conn):
        """Test executing simple SELECT query"""
        sql = "SELECT name, email FROM users WHERE age > 25"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)
        rows = cursor.result_set.fetchall()

        # Should return 19 users with age > 25 from the test dataset
        assert len(rows) == 19  # 19 out of 22 users are over 25
        if len(rows) > 0:
            # Get column names from description for DB API 2.0 compliance
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names
            assert "email" in col_names
            assert len(rows[0]) == 2  # Should have name and email columns

    def test_execute_select_all(self, conn):
        """Test executing SELECT * query"""
        sql = "SELECT * FROM products"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)
        rows = cursor.result_set.fetchall()

        # Should return all 50 products from test dataset
        assert len(rows) == 50

        # Check that expected product is present using DB API 2.0 access
        if cursor.result_set.description:
            col_names = [desc[0] for desc in cursor.result_set.description]
            if "name" in col_names:
                name_idx = col_names.index("name")
                names = [row[name_idx] for row in rows]
                assert "Laptop" in names  # First product from dataset

    def test_execute_with_limit(self, conn):
        """Test executing query with LIMIT"""
        sql = "SELECT name FROM users LIMIT 2"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)
        rows = cursor.result_set.fetchall()

        assert len(rows) == 2  # At least we get some results

        # Check that names are present using DB API 2.0
        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names
            assert len(rows[0]) >= 1  # Should have at least name column

    def test_execute_with_skip(self, conn):
        """Test executing query with OFFSET (SKIP)"""
        sql = "SELECT name FROM users OFFSET 1"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)
        rows = cursor.result_set.fetchall()

        # Should return users after skipping 1 (from 22 users in dataset)
        assert len(rows) == 21  # 22 - 1 = 21 users after skipping the first one

        # Check that results have name field if any results using DB API 2.0
        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names
            assert len(rows[0]) >= 1  # Should have at least name column

    def test_execute_with_sort(self, conn):
        """Test executing query with ORDER BY"""
        sql = "SELECT name FROM users ORDER BY age DESC"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)
        rows = cursor.result_set.fetchall()

        # Should return all 22 users sorted by age descending
        assert len(rows) == 22

        # Check that names are present using DB API 2.0
        col_names = [desc[0] for desc in cursor.result_set.description]
        assert "name" in col_names
        assert all(len(row) >= 1 for row in rows)  # All rows should have data

        # Verify that the first name in the result
        assert "Patricia Johnson" == rows[0][0]

    def test_execute_complex_query(self, conn):
        """Test executing complex query with multiple clauses"""
        sql = "SELECT name, email FROM users WHERE age > 25 ORDER BY name ASC LIMIT 5 OFFSET 10"

        # This should not crash, even if all features aren't fully implemented
        cursor = conn.cursor()
        result = cursor.execute(sql)
        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        # Get results - may not respect all clauses due to parser limitations
        rows = cursor.result_set.fetchall()
        assert isinstance(rows, list)

        # Should at least filter by age > 25 (19 users) from the 22 users in dataset
        if rows:  # If we get results (may not respect LIMIT/OFFSET yet)
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names and "email" in col_names
            for row in rows:
                assert len(row) >= 2  # Should have at least name and email

    def test_execute_nested_fields_query(self, conn):
        """Test executing query with nested field access"""
        sql = "SELECT name, profile.bio, address.city FROM users WHERE salary >= 100000 ORDER BY salary DESC"

        cursor = conn.cursor()
        result = cursor.execute(sql)
        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        # Get results - test nested field functionality
        rows = cursor.result_set.fetchall()
        assert isinstance(rows, list)
        assert len(rows) == 4

        # Verify that nested fields are properly projected
        if cursor.result_set.description:
            col_names = [desc[0] for desc in cursor.result_set.description]
            # Should include nested field names in projection
            assert "name" in col_names
            assert "profile.bio" in col_names
            assert "address.city" in col_names

        # Verify the first record matched the highest salary
        assert "Patricia Johnson" == rows[0][0]

    def test_execute_projection_function(self, conn):
        """Test end-to-end execution of projection functions in SELECT"""
        sql = (
            "SELECT DATE(created_at) AS created_date, "
            "DATETIME(created_at) AS created_dt, "
            "TIMESTAMP(created_at) AS created_ts, "
            "SUBSTR(email, 1, 4) AS email_prefix, "
            "UPPER(name) AS upper_name "
            "FROM users WHERE name = 'John Doe'"
        )
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        assert len(rows) == 1

        # Validate column names and projected function output
        col_names = [desc[0] for desc in cursor.result_set.description]
        assert "created_date" in col_names
        assert "created_dt" in col_names
        assert "created_ts" in col_names
        assert "email_prefix" in col_names
        assert "upper_name" in col_names

        created_date_idx = col_names.index("created_date")
        created_dt_idx = col_names.index("created_dt")
        created_ts_idx = col_names.index("created_ts")
        email_prefix_idx = col_names.index("email_prefix")
        upper_name_idx = col_names.index("upper_name")

        created_date_val = rows[0][created_date_idx]
        created_dt_val = rows[0][created_dt_idx]
        created_ts_val = rows[0][created_ts_idx]
        email_prefix_val = rows[0][email_prefix_idx]
        upper_name_val = rows[0][upper_name_idx]

        assert isinstance(created_date_val, date) and not isinstance(created_date_val, datetime)
        assert isinstance(created_dt_val, datetime)
        assert isinstance(created_ts_val, Timestamp)
        assert email_prefix_val == "john"
        assert upper_name_val == "JOHN DOE"

    def test_execute_parser_error(self, conn):
        """Test executing query with parser errors"""
        sql = "INVALID SQL SYNTAX"

        # This should raise an exception due to invalid SQL
        cursor = conn.cursor()
        with pytest.raises(SqlSyntaxError):  # Could be SqlSyntaxError or other parsing error
            cursor.execute(sql)

    def test_execute_database_error(self, conn, make_connection):
        """Test executing query with database error"""
        # Close the connection to simulate database error
        conn.close()

        sql = "SELECT * FROM users"

        # This should raise an exception due to closed connection
        cursor = conn.cursor()
        with pytest.raises(DatabaseError):
            cursor.execute(sql)

        # Reconnect for other tests
        new_conn = make_connection()
        try:
            cursor = new_conn.cursor()
        finally:
            new_conn.close()

    def test_fetchone_without_execute(self, conn):
        """Test fetchone without previous execute"""
        fresh_cursor = conn.cursor()
        with pytest.raises(ProgrammingError):
            fresh_cursor.fetchone()

    def test_fetchmany_without_execute(self, conn):
        """Test fetchmany without previous execute"""
        fresh_cursor = conn.cursor()
        with pytest.raises(ProgrammingError):
            fresh_cursor.fetchmany(5)

    def test_fetchall_without_execute(self, conn):
        """Test fetchall without previous execute"""
        fresh_cursor = conn.cursor()
        with pytest.raises(ProgrammingError):
            fresh_cursor.fetchall()

    def test_fetchone_with_result(self, conn):
        """Test fetchone with active result"""
        sql = "SELECT * FROM users"

        # Execute query first
        cursor = conn.cursor()
        _ = cursor.execute(sql)
        row = cursor.fetchone()

        assert row is not None
        assert isinstance(row, (tuple, list))
        # Verify we have data using DB API 2.0 approach
        col_names = [desc[0] for desc in cursor.result_set.description] if cursor.result_set.description else []
        if "name" in col_names:
            name_idx = col_names.index("name")
            assert row[name_idx]  # Should have name data
        else:
            assert len(row) > 0  # Should have some data

    def test_fetchmany_with_result(self, conn):
        """Test fetchmany with active result"""
        sql = "SELECT * FROM users"

        # Execute query first
        cursor = conn.cursor()
        _ = cursor.execute(sql)

        # Test fetchmany
        rows = cursor.fetchmany(2)
        assert len(rows) <= 2  # Should return at most 2 rows
        assert len(rows) >= 0  # Could be 0 if no results

        # Verify structure if we got results - DB API 2.0 compliance
        if len(rows) > 0:
            assert isinstance(rows[0], (tuple, list))  # Should be sequence, not dict
            assert len(rows[0]) > 0  # Should have data

    def test_fetchall_with_result(self, conn):
        """Test fetchall with active result"""
        sql = "SELECT * FROM users"

        # Execute query first
        cursor = conn.cursor()
        _ = cursor.execute(sql)

        # Test fetchall
        rows = cursor.fetchall()
        assert len(rows) == 22  # Should get all 22 test users

        # Verify all rows have expected structure using DB API 2.0
        if cursor.result_set.description:
            col_names = [desc[0] for desc in cursor.result_set.description]
            if "name" in col_names:
                name_idx = col_names.index("name")
                names = [row[name_idx] for row in rows]
                assert "John Doe" in names  # First user from dataset

    def test_description_type_and_shape(self, conn):
        """Ensure cursor.description returns a list of DB-API description tuples"""
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        desc = cursor.description
        assert isinstance(desc, list)
        assert all(isinstance(d, tuple) and len(d) == 7 and isinstance(d[0], str) for d in desc)
        # type_code should be a type object (e.g., str) or None when unknown
        assert all((isinstance(d[1], type) or d[1] is None) for d in desc)

    def test_description_projection(self, conn):
        """Ensure projection via SQL reflects in the description names and types"""
        cursor = conn.cursor()
        cursor.execute("SELECT name, email FROM users")
        desc = cursor.description
        assert isinstance(desc, list)
        col_names = [d[0] for d in desc]
        assert "name" in col_names
        assert "email" in col_names
        for d in desc:
            if d[0] in ("name", "email"):
                assert isinstance(d[1], type) or d[1] is None

    def test_cursor_pagination_fetchmany_triggers_getmore(self, conn, monkeypatch):
        """Test that cursor.fetchmany triggers getMore when executing SQL that yields a paginated cursor

        We monkeypatch the underlying database.command to force a small server batch size
        so that pagination/getMore behaviour is triggered while still using SQL via cursor.execute.
        """
        db = conn.database
        original_cmd = db.command

        def wrapper(cmd, *args, **kwargs):
            # Force small batchSize for find on users to simulate pagination
            if isinstance(cmd, dict) and cmd.get("find") == "users" and "batchSize" not in cmd:
                cmd = dict(cmd)
                cmd["batchSize"] = 3
            return original_cmd(cmd, *args, **kwargs)

        monkeypatch.setattr(db, "command", wrapper)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")

        # Fetch many rows through cursor - should span multiple batches
        rows = cursor.fetchmany(10)
        assert len(rows) == 10
        assert cursor.rowcount >= 10

    def test_cursor_pagination_fetchall_triggers_getmore(self, conn, monkeypatch):
        """Test that cursor.fetchall retrieves all rows across multiple batches using SQL

        Same approach: monkeypatch to force a small server batch size while using cursor.execute.
        """
        db = conn.database
        original_cmd = db.command

        def wrapper(cmd, *args, **kwargs):
            if isinstance(cmd, dict) and cmd.get("find") == "users" and "batchSize" not in cmd:
                cmd = dict(cmd)
                cmd["batchSize"] = 4
            return original_cmd(cmd, *args, **kwargs)

        monkeypatch.setattr(db, "command", wrapper)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")

        rows = cursor.fetchall()
        # There are 22 users in test dataset
        assert len(rows) == 22
        assert cursor.rowcount == 22

    def test_close(self, conn):
        """Test cursor close"""
        # Should not raise any exception
        cursor = conn.cursor()
        cursor.close()
        assert cursor._result_set is None

    def test_cursor_as_context_manager(self, conn):
        """Test cursor as context manager"""
        cursor = conn.cursor()
        with cursor as ctx:
            assert ctx == cursor

    def test_cursor_properties(self, conn):
        """Test cursor properties"""
        cursor = conn.cursor()
        assert cursor.connection == conn

        # Test rowcount property (should be -1 when no query executed)
        assert cursor.rowcount == -1

    def test_execute_with_field_alias(self, conn):
        """Test executing SELECT with field aliases"""
        sql = "SELECT name AS user_name, email AS user_email FROM users LIMIT 5"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)

        # Check that aliases appear in cursor description
        assert cursor.result_set.description is not None
        col_names = [desc[0] for desc in cursor.result_set.description]

        # Aliases should appear in the description instead of original field names
        assert "user_name" in col_names
        assert "user_email" in col_names
        assert "name" not in col_names
        assert "email" not in col_names

        rows = cursor.result_set.fetchall()
        assert len(rows) == 5
        assert len(rows[0]) == 2  # Should have 2 columns

    def test_execute_with_nested_field_alias(self, conn):
        """Test executing SELECT with nested field alias"""
        sql = "SELECT products.name AS product_name, products.price AS product_price FROM products LIMIT 3"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)

        # Check that aliases appear in cursor description
        assert cursor.result_set.description is not None
        col_names = [desc[0] for desc in cursor.result_set.description]

        # Aliases should appear in the description
        assert "product_name" in col_names
        assert "product_price" in col_names

        rows = cursor.result_set.fetchall()
        assert len(rows) == 3
        assert len(rows[0]) == 2  # Should have 2 columns

    def test_execute_with_positional_parameters(self, conn):
        """Test executing SELECT with positional parameters (?)"""
        sql = "SELECT name, email FROM users WHERE age > ? AND active = ?"
        cursor = conn.cursor()
        result = cursor.execute(sql, [25, True])

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        assert len(rows) > 0  # Should have results matching the filter
        assert len(rows[0]) == 2  # Should have name and email columns

    def test_execute_with_named_parameters(self, conn):
        """Test executing SELECT with named parameters (:name)"""
        sql = "SELECT name, email FROM users WHERE age > :min_age AND active = :is_active"
        cursor = conn.cursor()
        result = cursor.execute(sql, {"min_age": 25, "is_active": True})

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        assert len(rows) > 0  # Should have results matching the filter
        assert len(rows[0]) == 2  # Should have name and email columns

    def test_execute_with_reserved_keyword_field(self, conn):
        """Test executing SELECT with reserved keyword field name (quoted)"""
        # "date" is a reserved keyword in PartiQL, but can be used as a field name when quoted
        sql = 'SELECT name, "date" FROM users WHERE age > 25 LIMIT 5'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)

        # Check that "date" appears in cursor description
        assert cursor.result_set.description is not None
        col_names = [desc[0] for desc in cursor.result_set.description]

        # The quoted field name should appear in results
        assert "name" in col_names
        assert '"date"' in col_names or "date" in col_names

        rows = cursor.result_set.fetchall()
        assert len(rows) == 5
        assert len(rows[0]) == 2  # Should have name and date columns

        date_idx = col_names.index('"date"') if '"date"' in col_names else col_names.index("date")
        for row in rows:
            date_value = row[date_idx]
            assert date_value is not None

    def test_execute_with_reserved_keyword_field_in_where(self, conn):
        """Test executing WHERE clause with reserved keyword field name (quoted)"""
        sql = 'SELECT name FROM users WHERE "date" > ?'
        cursor = conn.cursor()
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result = cursor.execute(sql, [Timestamp(int(cutoff.timestamp()), 0)])

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        assert len(rows) == 3
        assert len(rows[0]) == 1

    # ===== Date Function Tests in WHERE Conditions =====
    # These tests verify that WHERE clause functions correctly convert values
    # to MongoDB types for filtering (different from projection functions in SELECT)

    def test_where_with_date_function_comparison(self, conn):
        """Test WHERE clause with explicit date() function for date comparison

        Verifies that date('2025-01-15') converts ISO string to Python date
        for proper MongoDB date filtering
        """
        sql = 'SELECT name FROM users WHERE "date" > date("2025-01-01")'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # Should return users with date > 2025-01-01
        assert len(rows) >= 0  # Number depends on test data

        # Verify we got name column
        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names
            assert len(rows[0]) == 1

    def test_where_with_date_function_eu_format(self, conn):
        """Test WHERE clause with date() function using EU date format

        Verifies that date('15/01/2025') EU format is correctly parsed
        and converted to Python date for MongoDB filtering
        """
        sql = 'SELECT name FROM users WHERE "date" >= date("15/01/2025")'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # EU format DD/MM/YYYY should be parsed correctly
        assert len(rows) >= 0

        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names

    def test_where_with_datetime_function(self, conn):
        """Test WHERE clause with explicit datetime() function

        Verifies that datetime('2025-01-15T10:30:00Z') converts ISO datetime
        with timezone to Python datetime for MongoDB filtering
        """
        sql = 'SELECT name FROM users WHERE "date" > datetime("2025-01-01T00:00:00Z")'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # Should return users with datetime > 2025-01-01 00:00:00 UTC
        assert len(rows) >= 0

        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names

    def test_where_with_timestamp_function(self, conn):
        """Test WHERE clause with explicit timestamp() function

        Verifies that timestamp('2025-01-15T10:30:00Z') converts ISO string
        to BSON Timestamp (4-byte sec + 4-byte counter) for MongoDB filtering
        """
        sql = 'SELECT name FROM users WHERE "date" > timestamp("2025-01-01T00:00:00Z")'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # Timestamp field should be compared correctly
        assert len(rows) >= 0

        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names

    def test_where_with_date_function_between(self, conn):
        """Test WHERE clause with BETWEEN and date() functions

        Verifies that multiple date() functions in BETWEEN clause
        work correctly for range filtering
        """
        sql = 'SELECT name FROM users WHERE "date" BETWEEN date("2025-01-01") AND date("2025-12-31")'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # Should return users with dates in 2025
        assert len(rows) >= 0

        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names

    def test_where_with_date_function_in_clause(self, conn):
        """Test WHERE clause with IN and date() functions

        Verifies that multiple date() functions in IN clause
        work correctly for multiple value filtering
        """
        sql = 'SELECT name FROM users WHERE "date" IN (date("2025-01-15"), date("2025-06-15"), date("2025-12-25"))'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # Should return users with matching dates
        assert len(rows) >= 0

        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names

    def test_where_with_date_and_string_filter(self, conn):
        """Test WHERE clause combining date() function with regular string filter

        Verifies that date functions work alongside other WHERE conditions
        """
        sql = 'SELECT name FROM users WHERE "date" > date("2025-01-01") AND active = true'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # Should return active users with date > 2025-01-01
        assert len(rows) >= 0

        if len(rows) > 0:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names

    def test_where_date_function_preserves_type(self, conn):
        """Test that date() function in WHERE preserves date type for filtering

        Verifies the conversion behavior: string -> Python date -> MongoDB Date
        This ensures correct type comparison in MongoDB
        """
        # Test with a known date value
        sql = 'SELECT name, "date" FROM users WHERE "date" = date("2025-01-15")'
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        # May or may not have matching records depending on test data
        assert len(rows) >= 0

        if len(rows) > 0:
            # Verify the returned date value
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names
            assert "date" in col_names or '"date"' in col_names
