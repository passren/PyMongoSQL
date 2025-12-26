# -*- coding: utf-8 -*-
from pymongosql.cursor import DictCursor
from pymongosql.result_set import DictResultSet


class TestDictCursor:
    """Test suite for DictCursor class - returns results as dictionaries"""

    def test_dict_cursor_init(self, conn):
        """Test DictCursor initialization"""
        dict_cursor = conn.cursor(DictCursor)
        assert dict_cursor._connection == conn
        assert dict_cursor._result_set is None
        assert dict_cursor._result_set_class == DictResultSet

    def test_dict_cursor_simple_select(self, conn):
        """Test DictCursor returning results as dictionaries"""
        sql = "SELECT name, email FROM users WHERE age > 25"
        dict_cursor = conn.cursor(DictCursor)
        result = dict_cursor.execute(sql)

        assert result == dict_cursor  # execute returns self
        assert isinstance(dict_cursor.result_set, DictResultSet)
        rows = dict_cursor.result_set.fetchall()

        # Should return 19 users with age > 25
        assert len(rows) == 19

        # Results should be dictionaries with field names as keys
        if len(rows) > 0:
            first_row = rows[0]
            assert isinstance(first_row, dict)
            assert "name" in first_row
            assert "email" in first_row
            # Should have exactly 2 keys
            assert len(first_row) == 2
            # Verify actual data values
            assert first_row["name"] == "John Doe"
            assert first_row["email"] == "john@example.com"

    def test_dict_cursor_select_all(self, conn):
        """Test DictCursor with SELECT *"""
        sql = "SELECT * FROM products LIMIT 3"
        dict_cursor = conn.cursor(DictCursor)
        dict_cursor.execute(sql)
        rows = dict_cursor.result_set.fetchall()

        assert len(rows) <= 3

        # All results should be dictionaries
        for row in rows:
            assert isinstance(row, dict)
            assert len(row) > 0  # Should have fields

    def test_dict_cursor_fetchone(self, conn):
        """Test DictCursor fetchone returns dictionary"""
        sql = "SELECT name, age FROM users"
        dict_cursor = conn.cursor(DictCursor)
        dict_cursor.execute(sql)

        row = dict_cursor.fetchone()

        assert row is not None
        assert isinstance(row, dict)
        assert "name" in row
        assert "age" in row
        # Verify actual data values
        assert row["name"] == "John Doe"
        assert row["age"] == 30

    def test_dict_cursor_fetchmany(self, conn):
        """Test DictCursor fetchmany returns list of dictionaries"""
        sql = "SELECT name, email FROM users ORDER BY name"
        dict_cursor = conn.cursor(DictCursor)
        dict_cursor.execute(sql)

        rows = dict_cursor.fetchmany(3)

        assert len(rows) == 3
        # All rows should be dictionaries
        for row in rows:
            assert isinstance(row, dict)
            assert "name" in row
            assert "email" in row
        # Verify actual data values
        assert rows[0]["name"] == "Alice Williams"
        assert rows[0]["email"] == "alice@example.com"
        assert rows[1]["name"] == "Bob Johnson"
        assert rows[2]["name"] == "Broken Reference User"

    def test_dict_cursor_with_where_clause(self, conn):
        """Test DictCursor with WHERE clause"""
        sql = "SELECT name, status FROM users WHERE age > 30 ORDER BY name"
        dict_cursor = conn.cursor(DictCursor)
        dict_cursor.execute(sql)

        rows = dict_cursor.fetchall()

        # Results should be dictionaries and all have age > 30
        assert len(rows) == 11
        if len(rows) > 0:
            for row in rows:
                assert isinstance(row, dict)
                assert "name" in row
                assert "status" in row
        # Verify actual data values
        assert rows[0]["name"] == "Bob Johnson"
        assert rows[0]["status"] is None

    def test_dict_cursor_with_order_by(self, conn):
        """Test DictCursor with ORDER BY"""
        sql = "SELECT name FROM users ORDER BY age DESC LIMIT 1"
        dict_cursor = conn.cursor(DictCursor)
        dict_cursor.execute(sql)

        rows = dict_cursor.fetchall()

        assert len(rows) == 1
        assert isinstance(rows[0], dict)
        assert "name" in rows[0]
        assert rows[0]["name"] == "Patricia Johnson"  # Highest age

    def test_dict_cursor_vs_tuple_cursor(self, conn):
        """Test that DictCursor returns dicts while regular Cursor returns tuples"""
        sql = "SELECT name, email FROM users LIMIT 1"

        # Get result from regular cursor (tuple)
        cursor = conn.cursor()
        cursor.execute(sql)
        tuple_row = cursor.fetchone()

        # Get result from dict cursor (dict)
        dict_cursor = conn.cursor(DictCursor)
        dict_cursor.execute(sql)
        dict_row = dict_cursor.fetchone()

        # Regular cursor returns tuple/sequence
        assert isinstance(tuple_row, (tuple, list))

        # DictCursor returns dictionary
        assert isinstance(dict_row, dict)

        # Both should have same data (just different formats)
        col_names = [desc[0] for desc in cursor.result_set.description]
        assert "name" in col_names
        assert "email" in col_names
        assert "name" in dict_row
        assert "email" in dict_row
        # Verify actual data matches between both cursor types
        assert dict_row["name"] == tuple_row[col_names.index("name")]
        assert dict_row["email"] == tuple_row[col_names.index("email")]
        assert dict_row["name"] == "John Doe"
        assert dict_row["email"] == "john@example.com"

    def test_dict_cursor_close(self, conn):
        """Test DictCursor close"""
        dict_cursor = conn.cursor(DictCursor)
        dict_cursor.execute("SELECT * FROM users LIMIT 1")
        dict_cursor.close()
        assert dict_cursor._result_set is None

    def test_dict_cursor_context_manager(self, conn):
        """Test DictCursor as context manager"""
        dict_cursor = conn.cursor(DictCursor)
        with dict_cursor as ctx:
            assert ctx == dict_cursor
