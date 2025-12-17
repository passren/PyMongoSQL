# -*- coding: utf-8 -*-
import pytest

from pymongosql.error import ProgrammingError
from pymongosql.result_set import ResultSet
from pymongosql.sql.builder import QueryPlan


class TestResultSet:
    """Test suite for ResultSet class"""

    # Shared projections used by tests
    PROJECTION_WITH_ALIASES = {"name": "full_name", "email": "user_email"}
    PROJECTION_EMPTY = {}

    def test_result_set_init(self, conn):
        """Test ResultSet initialization with command result"""
        db = conn.database
        # Execute a real command to get results
        command_result = db.command({"find": "users", "filter": {"age": {"$gt": 25}}, "limit": 1})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_WITH_ALIASES)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        assert result_set._command_result == command_result
        assert result_set._query_plan == query_plan
        assert result_set._is_closed is False

    def test_result_set_init_empty_projection(self, conn):
        """Test ResultSet initialization with empty projection"""
        db = conn.database
        command_result = db.command({"find": "users", "limit": 1})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        assert result_set._query_plan.projection_stage == {}

    def test_fetchone_with_data(self, conn):
        """Test fetchone with available data"""
        db = conn.database
        # Get real user data with projection mapping
        command_result = db.command({"find": "users", "projection": {"name": 1, "email": 1}, "limit": 1})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_WITH_ALIASES)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        row = result_set.fetchone()

        # Should apply projection mapping and return real data
        assert row is not None
        assert "full_name" in row  # Mapped from "name"
        assert "user_email" in row  # Mapped from "email"
        assert isinstance(row["full_name"], str)
        assert isinstance(row["user_email"], str)

    def test_fetchone_no_data(self, conn):
        """Test fetchone when no data available"""
        db = conn.database
        # Query for non-existent data
        command_result = db.command(
            {"find": "users", "filter": {"age": {"$gt": 999}}, "limit": 1}  # No users over 999 years old
        )

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_WITH_ALIASES)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        row = result_set.fetchone()

        assert row is None

    def test_fetchone_empty_projection(self, conn):
        """Test fetchone with empty projection (SELECT *)"""
        db = conn.database
        command_result = db.command({"find": "users", "limit": 1, "sort": {"_id": 1}})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        row = result_set.fetchone()

        # Should return original document without projection mapping
        assert row is not None
        assert "_id" in row
        assert "name" in row  # Original field names
        assert "email" in row
        # Should be "John Doe" from test dataset
        assert "John Doe" in row["name"]

    def test_fetchone_closed_cursor(self, conn):
        """Test fetchone on closed cursor"""
        db = conn.database
        command_result = db.command({"find": "users", "limit": 1})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_WITH_ALIASES)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        result_set.close()

        with pytest.raises(ProgrammingError, match="ResultSet is closed"):
            result_set.fetchone()

    def test_fetchmany_with_data(self, conn):
        """Test fetchmany with available data"""
        db = conn.database
        # Get multiple users with projection
        command_result = db.command({"find": "users", "projection": {"name": 1, "email": 1}, "limit": 5})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_WITH_ALIASES)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        rows = result_set.fetchmany(2)

        assert len(rows) <= 2  # Should return at most 2 rows
        assert len(rows) >= 1  # Should have at least 1 row from test data

        # Check projection mapping
        for row in rows:
            assert "full_name" in row  # Mapped from "name"
            assert "user_email" in row  # Mapped from "email"
            assert isinstance(row["full_name"], str)
            assert isinstance(row["user_email"], str)

    def test_fetchmany_default_size(self, conn):
        """Test fetchmany with default size"""
        db = conn.database
        # Get all users (22 total in test dataset)
        command_result = db.command({"find": "users"})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        rows = result_set.fetchmany()  # Should use default arraysize (1000)

        assert len(rows) == 22  # Gets all available users since arraysize (1000) > available (22)

    def test_fetchmany_less_data_available(self, conn):
        """Test fetchmany when less data available than requested"""
        db = conn.database
        # Get only 2 users but request 5
        command_result = db.command({"find": "users", "limit": 2})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        rows = result_set.fetchmany(5)  # Request 5 but only 2 available

        assert len(rows) == 2

    def test_fetchmany_no_data(self, conn):
        """Test fetchmany when no data available"""
        db = conn.database
        # Query for non-existent data
        command_result = db.command({"find": "users", "filter": {"age": {"$gt": 999}}})  # No users over 999 years old

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        rows = result_set.fetchmany(3)

        assert rows == []

    def test_fetchall_with_data(self, conn):
        """Test fetchall with available data"""
        db = conn.database
        # Get users over 25 (should be 19 users from test dataset)
        command_result = db.command(
            {"find": "users", "filter": {"age": {"$gt": 25}}, "projection": {"name": 1, "email": 1}}
        )

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_WITH_ALIASES)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        rows = result_set.fetchall()

        assert len(rows) == 19  # 19 users over 25 from test dataset

        # Check first row has proper projection mapping
        assert "full_name" in rows[0]  # Mapped from "name"
        assert "user_email" in rows[0]  # Mapped from "email"
        assert isinstance(rows[0]["full_name"], str)
        assert isinstance(rows[0]["user_email"], str)

    def test_fetchall_no_data(self, conn):
        """Test fetchall when no data available"""
        db = conn.database
        command_result = db.command({"find": "users", "filter": {"age": {"$gt": 999}}})  # No users over 999 years old

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        rows = result_set.fetchall()

        assert rows == []

    def test_fetchall_closed_cursor(self, conn):
        """Test fetchall on closed cursor"""
        db = conn.database
        command_result = db.command({"find": "users", "limit": 1})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        result_set.close()

        with pytest.raises(ProgrammingError, match="ResultSet is closed"):
            result_set.fetchall()

    def test_apply_projection_mapping(self):
        """Test _process_document method"""
        projection = {"name": "full_name", "age": "user_age", "email": "email"}
        query_plan = QueryPlan(collection="users", projection_stage=projection)

        # Create empty command result for testing _process_document method
        command_result = {"cursor": {"firstBatch": []}}
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        doc = {
            "_id": "123",
            "name": "John",
            "age": 30,
            "email": "john@example.com",
            "extra": "ignored",
        }

        mapped_doc = result_set._process_document(doc)

        expected = {"full_name": "John", "user_age": 30, "email": "john@example.com"}
        assert mapped_doc == expected

    def test_apply_projection_mapping_missing_fields(self):
        """Test projection mapping with missing fields in document"""
        projection = {
            "name": "full_name",
            "age": "user_age",
            "missing": "missing_alias",
        }
        query_plan = QueryPlan(collection="users", projection_stage=projection)

        command_result = {"cursor": {"firstBatch": []}}
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        doc = {"_id": "123", "name": "John"}  # Missing age and missing fields

        mapped_doc = result_set._process_document(doc)

        # Should include mapped fields and None for missing fields
        expected = {"full_name": "John", "user_age": None, "missing_alias": None}
        assert mapped_doc == expected

    def test_apply_projection_mapping_identity_mapping(self):
        """Test projection mapping with identity mapping (field: field)"""
        projection = {"name": "name", "age": "age"}
        query_plan = QueryPlan(collection="users", projection_stage=projection)

        command_result = {"cursor": {"firstBatch": []}}
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        doc = {"_id": "123", "name": "John", "age": 30}

        mapped_doc = result_set._process_document(doc)

        expected = {"name": "John", "age": 30}
        assert mapped_doc == expected

    def test_close(self):
        """Test close method"""
        command_result = {"cursor": {"firstBatch": []}}
        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        # Should not be closed initially
        assert not result_set._is_closed

        result_set.close()

        # Should be closed after calling close
        assert result_set._is_closed

    def test_context_manager(self):
        """Test ResultSet as context manager"""
        command_result = {"cursor": {"firstBatch": []}}
        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        with result_set as rs:
            assert rs == result_set
            assert not rs._is_closed

        # Should be closed after exiting context
        assert result_set._is_closed

    def test_context_manager_with_exception(self):
        """Test context manager with exception"""
        command_result = {"cursor": {"firstBatch": []}}
        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        try:
            with result_set as rs:
                assert not rs._is_closed
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Should still be closed after exception
        assert result_set._is_closed

    def test_iterator_protocol(self, conn):
        """Test ResultSet as iterator"""
        db = conn.database
        # Get 2 users from database
        command_result = db.command({"find": "users", "limit": 2})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        # Test iterator protocol
        iterator = iter(result_set)
        assert iterator == result_set

        # Test iteration
        rows = list(result_set)
        assert len(rows) == 2
        assert "_id" in rows[0]
        assert "name" in rows[0]

    def test_iterator_with_projection(self, conn):
        """Test iteration with projection mapping"""
        db = conn.database
        command_result = db.command({"find": "users", "projection": {"name": 1, "email": 1}, "limit": 2})

        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_WITH_ALIASES)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        rows = list(result_set)
        assert len(rows) == 2
        assert "full_name" in rows[0]  # Mapped from "name"
        assert "user_email" in rows[0]  # Mapped from "email"

    def test_iterator_closed_cursor(self):
        """Test iteration on closed cursor"""
        command_result = {"cursor": {"firstBatch": []}}
        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)
        result_set.close()

        with pytest.raises(ProgrammingError, match="ResultSet is closed"):
            list(result_set)

    def test_arraysize_property(self):
        """Test arraysize property"""
        command_result = {"cursor": {"firstBatch": []}}
        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        # Default arraysize should be 1000
        assert result_set.arraysize == 1000

        # Should be able to change arraysize
        result_set.arraysize = 20
        assert result_set.arraysize == 20

    def test_arraysize_validation(self):
        """Test arraysize validation"""
        command_result = {"cursor": {"firstBatch": []}}
        query_plan = QueryPlan(collection="users", projection_stage=self.PROJECTION_EMPTY)
        result_set = ResultSet(command_result=command_result, query_plan=query_plan)

        # Should reject invalid values
        with pytest.raises(ValueError, match="arraysize must be positive"):
            result_set.arraysize = 0

        with pytest.raises(ValueError, match="arraysize must be positive"):
            result_set.arraysize = -5
