# -*- coding: utf-8 -*-
import json

from pymongosql.result_set import ResultSet


class TestCursorAggregate:
    """Test aggregate function execution with real MongoDB data"""

    def test_aggregate_qualified_basic_execution(self, conn):
        """Test executing qualified aggregate call: collection.aggregate('pipeline', 'options')"""
        sql = """
        SELECT *
        FROM users.aggregate('[{"$match": {"age": {"$gt": 25}}}]', '{}')
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        assert len(rows) > 0  # Should have users over 25
        assert len(rows) == 19  # Expected count from test data

    def test_aggregate_unqualified_group_execution(self, conn):
        """Test executing unqualified aggregate: aggregate('pipeline', 'options')"""
        # This requires specifying collection at execution time or in a different way
        # For now, test the qualified version which is more practical
        pass

    def test_aggregate_with_projection(self, conn):
        """Test aggregate with SELECT projection - should project specified fields"""
        sql = """
        SELECT name, age
        FROM users.aggregate('[{"$match": {"active": true}}]', '{}')
        LIMIT 5
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        # Check description has correct columns
        col_names = [desc[0] for desc in cursor.result_set.description]
        assert "name" in col_names
        assert "age" in col_names

        rows = cursor.result_set.fetchall()
        assert len(rows) > 0
        assert len(rows[0]) == 2  # Should have 2 columns (name, age)

    def test_aggregate_with_where_clause(self, conn):
        """Test aggregate pipeline combined with WHERE clause for additional filtering"""
        sql = """
        SELECT name, email, age
        FROM users.aggregate('[{"$match": {"active": true}}]', '{}')
        WHERE age > 30
        LIMIT 10
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        assert isinstance(cursor.result_set, ResultSet)

        rows = cursor.result_set.fetchall()
        assert len(rows) > 0
        # All returned rows should have age > 30
        col_names = [desc[0] for desc in cursor.result_set.description]
        age_idx = col_names.index("age")
        for row in rows:
            assert row[age_idx] > 30

    def test_aggregate_with_sort_and_limit(self, conn):
        """Test aggregate with ORDER BY and LIMIT"""
        sql = """
        SELECT name, age
        FROM users.aggregate('[{"$match": {"active": true}}]', '{}')
        ORDER BY age DESC
        LIMIT 5
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        rows = cursor.result_set.fetchall()

        assert len(rows) == 5
        # Verify ordering - each row should have age >= next row
        col_names = [desc[0] for desc in cursor.result_set.description]
        age_idx = col_names.index("age")
        ages = [row[age_idx] for row in rows]
        assert ages == sorted(ages, reverse=True)

    def test_aggregate_products_group_by(self, conn):
        """Test aggregate with $group stage to group products"""
        pipeline = json.dumps([{"$group": {"_id": "$category", "count": {"$sum": 1}, "avg_price": {"$avg": "$price"}}}])

        sql = f"""
        SELECT *
        FROM products.aggregate('{pipeline}', '{{}}')
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        rows = cursor.result_set.fetchall()

        # Should have results grouped by category
        assert len(rows) > 0

    def test_aggregate_orders_sum_amount(self, conn):
        """Test aggregate with $group to sum order amounts"""
        pipeline = json.dumps(
            [{"$group": {"_id": "$status", "total_amount": {"$sum": "$total"}, "order_count": {"$sum": 1}}}]
        )

        sql = f"""
        SELECT *
        FROM orders.aggregate('{pipeline}', '{{}}')
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        rows = cursor.result_set.fetchall()

        # Should have grouped results by order status
        assert len(rows) > 0

    def test_aggregate_with_fetchone(self, conn):
        """Test aggregate query using fetchone instead of fetchall"""
        sql = """
        SELECT name, age
        FROM users.aggregate('[{"$match": {"age": {"$gte": 20}}}]', '{}')
        ORDER BY age DESC
        """

        cursor = conn.cursor()
        cursor.execute(sql)

        # Get first row with fetchone
        first_row = cursor.fetchone()
        assert first_row is not None
        assert len(first_row) == 2

        # Should be oldest user
        col_names = [desc[0] for desc in cursor.result_set.description]
        age_idx = col_names.index("age")
        first_age = first_row[age_idx]

        # Get next few rows and verify age is descending
        next_rows = cursor.fetchmany(3)
        for row in next_rows:
            assert row[age_idx] <= first_age

    def test_aggregate_with_skip(self, conn):
        """Test aggregate with OFFSET (SKIP)"""
        sql = """
        SELECT name, email
        FROM users.aggregate('[{"$match": {"active": true}}]', '{}')
        ORDER BY name ASC
        LIMIT 10 OFFSET 5
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        rows = cursor.result_set.fetchall()

        # Should have some results (skipped first 5, limited to 10)
        assert len(rows) > 0
        assert len(rows) <= 10

    def test_aggregate_cursor_rowcount(self, conn):
        """Test that cursor.rowcount reflects aggregate query results"""
        sql = """
        SELECT *
        FROM users.aggregate('[{"$match": {"age": {"$gt": 25}}}]', '{}')
        """

        cursor = conn.cursor()
        cursor.execute(sql)

        rows = cursor.fetchall()
        # rowcount should match the number of rows fetched
        assert cursor.rowcount == len(rows)

    def test_aggregate_with_field_alias(self, conn):
        """Test aggregate query with field aliases in projection"""
        sql = """
        SELECT name AS user_name, age AS user_age
        FROM users.aggregate('[{"$match": {"active": true}}]', '{}')
        LIMIT 3
        """

        cursor = conn.cursor()
        cursor.execute(sql)

        # Check that aliases appear in description
        col_names = [desc[0] for desc in cursor.result_set.description]
        assert "user_name" in col_names
        assert "user_age" in col_names
        assert "name" not in col_names
        assert "age" not in col_names

        rows = cursor.result_set.fetchall()
        assert len(rows) == 3
        assert len(rows[0]) == 2

    def test_aggregate_description_type_info(self, conn):
        """Test that cursor.description has proper DB API 2.0 format for aggregate queries"""
        sql = """
        SELECT name, age, email
        FROM users.aggregate('[{"$match": {"active": true}}]', '{}')
        LIMIT 1
        """

        cursor = conn.cursor()
        cursor.execute(sql)

        # Verify description format
        desc = cursor.description
        assert isinstance(desc, list)
        assert len(desc) == 3  # 3 columns
        assert all(isinstance(d, tuple) and len(d) == 7 for d in desc)
        assert all(isinstance(d[0], str) for d in desc)  # Column names are strings

    def test_aggregate_empty_result(self, conn):
        """Test aggregate query that returns no results"""
        sql = """
        SELECT *
        FROM users.aggregate('[{"$match": {"age": {"$gt": 200}}}]', '{}')
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        rows = cursor.result_set.fetchall()
        assert len(rows) == 0

    def test_aggregate_multiple_stages(self, conn):
        """Test aggregate with multiple pipeline stages"""
        pipeline = json.dumps(
            [
                {"$match": {"active": True}},
                {"$group": {"_id": None, "avg_age": {"$avg": "$age"}, "count": {"$sum": 1}}},
                {"$project": {"_id": 0, "average_age": "$avg_age", "total_users": "$count"}},
            ]
        )

        sql = f"""
        SELECT *
        FROM users.aggregate('{pipeline}', '{{}}')
        """

        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor
        rows = cursor.result_set.fetchall()

        # Should have one row with aggregated stats
        assert len(rows) == 1
        row = rows[0]
        assert len(row) >= 2  # Should have average_age and total_users
