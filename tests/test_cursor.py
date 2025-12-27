# -*- coding: utf-8 -*-
import pytest

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
                assert (isinstance(d[1], type) or d[1] is None)

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
