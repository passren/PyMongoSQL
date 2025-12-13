# -*- coding: utf-8 -*-
import pytest
from unittest.mock import Mock, MagicMock
from pymongosql.result_set import ResultSet
from pymongosql.error import OperationalError, ProgrammingError
from pymongosql.sql.builder import QueryPlan


class TestResultSet:
    """Test suite for ResultSet class"""

    def setup_method(self):
        """Setup for each test method"""
        self.mock_cursor = Mock()
        self.mock_projection = {"name": "full_name", "email": "user_email"}

        # Create QueryPlan objects for testing
        self.query_plan_with_projection = QueryPlan(
            collection="test_collection", projection_stage=self.mock_projection
        )

        self.query_plan_empty_projection = QueryPlan(
            collection="test_collection", projection_stage={}
        )

    def test_result_set_init(self):
        """Test ResultSet initialization"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_with_projection)
        assert result_set._mongo_cursor == self.mock_cursor
        assert result_set._query_plan == self.query_plan_with_projection
        assert result_set._is_closed == False

    def test_result_set_init_empty_projection(self):
        """Test ResultSet initialization with empty projection"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        assert result_set._query_plan.projection_stage == {}

    def test_fetchone_with_data(self):
        """Test fetchone with available data"""
        # Setup mock cursor to return data
        mock_doc = {"_id": "123", "name": "John", "email": "john@example.com"}
        self.mock_cursor.__iter__ = Mock(return_value=iter([mock_doc]))

        result_set = ResultSet(self.mock_cursor, self.query_plan_with_projection)
        row = result_set.fetchone()

        # Should apply projection mapping
        expected = {"full_name": "John", "user_email": "john@example.com"}
        assert row == expected

    def test_fetchone_no_data(self):
        """Test fetchone when no data available"""
        self.mock_cursor.__iter__ = Mock(return_value=iter([]))

        result_set = ResultSet(self.mock_cursor, self.query_plan_with_projection)
        row = result_set.fetchone()

        assert row is None

    def test_fetchone_empty_projection(self):
        """Test fetchone with empty projection (SELECT *)"""
        mock_doc = {"_id": "123", "name": "John", "email": "john@example.com"}
        self.mock_cursor.__iter__ = Mock(return_value=iter([mock_doc]))

        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        row = result_set.fetchone()

        # Should return original document
        assert row == mock_doc

    def test_fetchone_closed_cursor(self):
        """Test fetchone on closed cursor"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_with_projection)
        result_set.close()

        with pytest.raises(ProgrammingError, match="ResultSet is closed"):
            result_set.fetchone()

    def test_fetchmany_with_data(self):
        """Test fetchmany with available data"""
        mock_docs = [
            {"_id": "123", "name": "John", "email": "john@example.com"},
            {"_id": "456", "name": "Jane", "email": "jane@example.com"},
            {"_id": "789", "name": "Bob", "email": "bob@example.com"},
        ]

        # Mock iterator behavior
        mock_iter = iter(mock_docs)
        self.mock_cursor.__iter__ = Mock(return_value=mock_iter)

        result_set = ResultSet(self.mock_cursor, self.query_plan_with_projection)
        rows = result_set.fetchmany(2)

        assert len(rows) == 2
        assert rows[0] == {"full_name": "John", "user_email": "john@example.com"}
        assert rows[1] == {"full_name": "Jane", "user_email": "jane@example.com"}

    def test_fetchmany_default_size(self):
        """Test fetchmany with default size"""
        mock_docs = [{"_id": str(i), "name": f"User{i}"} for i in range(15)]

        # Mock iterator behavior
        mock_iter = iter(mock_docs)
        self.mock_cursor.__iter__ = Mock(return_value=mock_iter)

        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        rows = result_set.fetchmany()  # Should use default arraysize (1000)

        assert (
            len(rows) == 15
        )  # Gets all available docs since arraysize (1000) > available (15)

    def test_fetchmany_less_data_available(self):
        """Test fetchmany when less data available than requested"""
        mock_docs = [{"_id": "123", "name": "John"}, {"_id": "456", "name": "Jane"}]

        # Mock iterator behavior
        mock_iter = iter(mock_docs)
        self.mock_cursor.__iter__ = Mock(return_value=mock_iter)

        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        rows = result_set.fetchmany(5)  # Request 5 but only 2 available

        assert len(rows) == 2

    def test_fetchmany_no_data(self):
        """Test fetchmany when no data available"""
        mock_iter = iter([])
        self.mock_cursor.__iter__ = Mock(return_value=mock_iter)

        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        rows = result_set.fetchmany(3)

        assert rows == []

    def test_fetchall_with_data(self):
        """Test fetchall with available data"""
        mock_docs = [
            {"_id": "123", "name": "John", "email": "john@example.com"},
            {"_id": "456", "name": "Jane", "email": "jane@example.com"},
            {"_id": "789", "name": "Bob", "email": "bob@example.com"},
        ]

        # Mock list() behavior
        self.mock_cursor.__iter__ = Mock(return_value=iter(mock_docs))

        result_set = ResultSet(self.mock_cursor, self.query_plan_with_projection)
        rows = result_set.fetchall()

        assert len(rows) == 3
        assert rows[0] == {"full_name": "John", "user_email": "john@example.com"}
        assert rows[1] == {"full_name": "Jane", "user_email": "jane@example.com"}
        assert rows[2] == {"full_name": "Bob", "user_email": "bob@example.com"}

    def test_fetchall_no_data(self):
        """Test fetchall when no data available"""
        self.mock_cursor.__iter__ = Mock(return_value=iter([]))

        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        rows = result_set.fetchall()

        assert rows == []

    def test_fetchall_closed_cursor(self):
        """Test fetchall on closed cursor"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        result_set.close()

        with pytest.raises(ProgrammingError, match="ResultSet is closed"):
            result_set.fetchall()

    def test_apply_projection_mapping(self):
        """Test _process_document method"""
        projection = {"name": "full_name", "age": "user_age", "email": "email"}
        query_plan = QueryPlan(
            collection="test_collection", projection_stage=projection
        )
        doc = {
            "_id": "123",
            "name": "John",
            "age": 30,
            "email": "john@example.com",
            "extra": "ignored",
        }

        result_set = ResultSet(self.mock_cursor, query_plan)
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
        query_plan = QueryPlan(
            collection="test_collection", projection_stage=projection
        )
        doc = {"_id": "123", "name": "John"}  # Missing age and missing fields

        result_set = ResultSet(self.mock_cursor, query_plan)
        mapped_doc = result_set._process_document(doc)

        # Should include mapped fields and None for missing fields
        expected = {"full_name": "John", "user_age": None, "missing_alias": None}
        assert mapped_doc == expected

    def test_apply_projection_mapping_identity_mapping(self):
        """Test projection mapping with identity mapping (field: field)"""
        projection = {"name": "name", "age": "age"}
        query_plan = QueryPlan(
            collection="test_collection", projection_stage=projection
        )
        doc = {"_id": "123", "name": "John", "age": 30}

        result_set = ResultSet(self.mock_cursor, query_plan)
        mapped_doc = result_set._process_document(doc)

        expected = {"name": "John", "age": 30}
        assert mapped_doc == expected

    def test_close(self):
        """Test close method"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)

        # Should not be closed initially
        assert not result_set._is_closed

        result_set.close()

        # Should be closed after calling close
        assert result_set._is_closed

    def test_context_manager(self):
        """Test ResultSet as context manager"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)

        with result_set as rs:
            assert rs == result_set
            assert not rs._is_closed

        # Should be closed after exiting context
        assert result_set._is_closed

    def test_context_manager_with_exception(self):
        """Test context manager with exception"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)

        try:
            with result_set as rs:
                assert not rs._is_closed
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Should still be closed after exception
        assert result_set._is_closed

    def test_iterator_protocol(self):
        """Test ResultSet as iterator"""
        mock_docs = [{"_id": "123", "name": "John"}, {"_id": "456", "name": "Jane"}]

        # Mock iterator behavior
        self.mock_cursor.__iter__ = Mock(return_value=iter(mock_docs))

        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)

        # Test iterator protocol
        iterator = iter(result_set)
        assert iterator == result_set

        # Test iteration
        rows = list(result_set)
        assert len(rows) == 2
        assert rows[0] == {"_id": "123", "name": "John"}
        assert rows[1] == {"_id": "456", "name": "Jane"}

    def test_iterator_with_projection(self):
        """Test iteration with projection mapping"""
        mock_docs = [
            {"_id": "123", "name": "John", "email": "john@example.com"},
            {"_id": "456", "name": "Jane", "email": "jane@example.com"},
        ]

        self.mock_cursor.__iter__ = Mock(return_value=iter(mock_docs))

        result_set = ResultSet(self.mock_cursor, self.query_plan_with_projection)

        rows = list(result_set)
        assert len(rows) == 2
        assert rows[0] == {"full_name": "John", "user_email": "john@example.com"}
        assert rows[1] == {"full_name": "Jane", "user_email": "jane@example.com"}

    def test_iterator_closed_cursor(self):
        """Test iteration on closed cursor"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)
        result_set.close()

        with pytest.raises(ProgrammingError, match="ResultSet is closed"):
            list(result_set)

    def test_arraysize_property(self):
        """Test arraysize property"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)

        # Default arraysize should be 1000
        assert result_set.arraysize == 1000

        # Should be able to change arraysize
        result_set.arraysize = 20
        assert result_set.arraysize == 20

    def test_arraysize_validation(self):
        """Test arraysize validation"""
        result_set = ResultSet(self.mock_cursor, self.query_plan_empty_projection)

        # Should reject invalid values
        with pytest.raises(ValueError, match="arraysize must be positive"):
            result_set.arraysize = 0

        with pytest.raises(ValueError, match="arraysize must be positive"):
            result_set.arraysize = -5
