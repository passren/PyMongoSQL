# -*- coding: utf-8 -*-
from pymongosql.result_set import ResultSet
from pymongosql.sql.builder import BuilderFactory


class TestResultSetPagination:
    """Test suite for ResultSet pagination with getMore"""

    # Shared projections used by tests
    PROJECTION_WITH_FIELDS = {"name": 1, "email": 1}
    PROJECTION_EMPTY = {}

    def test_pagination_cursor_id_zero(self, conn):
        """Test pagination when cursor_id is 0 (all results in firstBatch)"""
        db = conn.database
        # Query with small limit - all results fit in firstBatch
        command_result = db.command({"find": "users", "limit": 5})

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_EMPTY).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        # Check cursor_id - should be 0 when all results fit in firstBatch
        assert result_set._cursor_id == 0
        assert result_set._cache_exhausted is False  # Not exhausted yet, but no getMore needed

        # Fetch all results
        rows = result_set.fetchall()
        assert len(rows) == 5

        # After fetching all, cache should be exhausted
        assert result_set._cache_exhausted is True

    def test_pagination_multiple_batches(self, conn):
        """Test pagination across multiple batches with getMore"""
        db = conn.database
        # Use a small batch size (batchSize) to force pagination
        command_result = db.command({"find": "users", "batchSize": 5})  # Only 5 results per batch

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_EMPTY).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        # Initial results should have cursor_id > 0 since we have 22 total users and batchSize is 5
        initial_cached = len(result_set._cached_results)
        assert initial_cached <= 5  # Should have at most 5 in cache from firstBatch

        # Fetch multiple results (should trigger getMore)
        rows = result_set.fetchmany(10)
        assert len(rows) == 10

        # After fetching, we should have processed multiple batches
        assert result_set._total_fetched >= 10

    def test_pagination_ensure_results_available(self, conn):
        """Test _ensure_results_available with pagination"""
        db = conn.database
        # Request results with small batch size
        command_result = db.command({"find": "users", "batchSize": 3})  # Small batch to test pagination

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_EMPTY).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        # Initially, cache might have 3 results
        initial_cache_size = len(result_set._cached_results)
        assert initial_cache_size <= 3

        # Ensure we have 8 results available - should trigger getMore
        result_set._ensure_results_available(8)
        assert len(result_set._cached_results) >= 8

        # Check that cursor_id was updated
        assert result_set._cursor_id >= 0

    def test_pagination_fetchone_triggers_getmore(self, conn):
        """Test that fetchone triggers getMore when needed"""
        db = conn.database
        # Create result set with small batch size
        command_result = db.command({"find": "users", "batchSize": 2})  # Very small batch

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_WITH_FIELDS).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        _ = result_set._cursor_id
        rows_fetched = []

        # Fetch many single rows - should trigger getMore multiple times
        for _ in range(10):
            row = result_set.fetchone()
            if row:
                rows_fetched.append(row)

        assert len(rows_fetched) == 10
        # rowcount should reflect total fetched
        assert result_set.rowcount >= 10

    def test_pagination_cache_exhausted_flag(self, conn):
        """Test cache exhausted flag is set correctly"""
        db = conn.database
        command_result = db.command({"find": "users", "limit": 3})

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_EMPTY).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        assert result_set._cache_exhausted is False

        # Fetch all results
        rows = result_set.fetchall()
        assert len(rows) == 3

        # After exhausting results, flag should be set
        assert result_set._cache_exhausted is True

        # Subsequent fetches should return empty
        more_rows = result_set.fetchall()
        assert more_rows == []

    def test_pagination_rowcount_tracking(self, conn):
        """Test rowcount is accurately tracked during pagination"""
        db = conn.database
        command_result = db.command({"find": "users", "batchSize": 4})

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_EMPTY).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        initial_rowcount = result_set.rowcount
        assert initial_rowcount <= 4  # Initial batch size

        # Fetch multiple batches
        batch1 = result_set.fetchmany(8)
        assert result_set.rowcount >= 8

        batch2 = result_set.fetchmany(5)
        assert result_set.rowcount >= 13

        # Fetch all remaining
        all_remaining = result_set.fetchall()
        _ = result_set.rowcount

        # All 22 users should be fetched eventually
        total_fetched = len(batch1) + len(batch2) + len(all_remaining)
        assert total_fetched == 22

    def test_pagination_with_projection(self, conn):
        """Test pagination with field projection applied"""
        db = conn.database
        command_result = db.command({"find": "users", "projection": {"name": 1, "email": 1}, "batchSize": 3})

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_WITH_FIELDS).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        # Fetch across multiple batches
        rows = result_set.fetchall()

        # Should have all 22 users
        assert len(rows) == 22

        # Each row should have exactly 2 projected fields
        col_names = [desc[0] for desc in result_set.description]
        for row in rows:
            assert len(row) == 2
            assert isinstance(row[col_names.index("name")], (str, type(None)))
            assert isinstance(row[col_names.index("email")], (str, type(None)))

    def test_pagination_fetchmany_across_batches(self, conn):
        """Test fetchmany that spans multiple getMore calls"""
        db = conn.database
        command_result = db.command({"find": "users", "batchSize": 3})

        execution_plan = (
            BuilderFactory.create_query_builder().collection("users").project(self.PROJECTION_EMPTY).build()
        )
        result_set = ResultSet(command_result=command_result, execution_plan=execution_plan, database=db)

        # Fetch 10 rows - should span multiple batches
        batch1 = result_set.fetchmany(10)
        assert len(batch1) == 10

        # Fetch next 10 - should get more users
        batch2 = result_set.fetchmany(10)
        assert len(batch2) == 10

        # Fetch remaining results
        batch3 = result_set.fetchmany(5)
        # Should get remaining users (total > 20, depends on actual data size)
        assert len(batch3) > 0
