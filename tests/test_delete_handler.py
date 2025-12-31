# -*- coding: utf-8 -*-
from pymongosql.sql.delete_handler import DeleteHandler, DeleteParseResult
from pymongosql.sql.partiql.PartiQLParser import PartiQLParser


class TestDeleteParseResult:
    """Test DeleteParseResult dataclass."""

    def test_for_visitor_factory(self):
        """Test factory method creates fresh instance."""
        result = DeleteParseResult.for_visitor()
        assert result.collection is None
        assert result.filter_conditions == {}
        assert result.has_errors is False
        assert result.error_message is None

    def test_validate_missing_collection(self):
        """Test validation fails when collection is missing."""
        result = DeleteParseResult()
        is_valid = result.validate()

        assert is_valid is False
        assert result.has_errors is True
        assert result.error_message == "Collection name is required"

    def test_validate_with_collection(self):
        """Test validation passes when collection is set."""
        result = DeleteParseResult(collection="test_collection")
        is_valid = result.validate()

        assert is_valid is True
        assert result.has_errors is False

    def test_to_dict(self):
        """Test to_dict conversion."""
        result = DeleteParseResult(
            collection="users", filter_conditions={"age": {"$gt": 25}}, has_errors=False, error_message=None
        )

        result_dict = result.to_dict()
        assert result_dict["collection"] == "users"
        assert result_dict["filter_conditions"] == {"age": {"$gt": 25}}
        assert result_dict["has_errors"] is False
        assert result_dict["error_message"] is None

    def test_repr(self):
        """Test string representation."""
        result = DeleteParseResult(collection="products", filter_conditions={"price": {"$lt": 100}}, has_errors=False)

        repr_str = repr(result)
        assert "DeleteParseResult" in repr_str
        assert "collection=products" in repr_str
        assert "has_errors=False" in repr_str


class TestDeleteHandler:
    """Test DeleteHandler class."""

    def test_can_handle_delete_context(self):
        """Test can_handle returns True for DELETE context."""
        handler = DeleteHandler()

        # Mock context with DELETE attribute
        class MockDeleteContext:
            def DELETE(self):
                return True

        ctx = MockDeleteContext()
        assert handler.can_handle(ctx) is True

    def test_can_handle_delete_command_context(self):
        """Test can_handle returns True for DeleteCommandContext."""
        handler = DeleteHandler()

        # Mock DeleteCommandContext
        class MockDeleteCommandContext(PartiQLParser.DeleteCommandContext):
            def __init__(self):
                pass  # Skip parent init

        ctx = MockDeleteCommandContext()
        assert handler.can_handle(ctx) is True

    def test_can_handle_non_delete_context(self):
        """Test can_handle returns False for non-DELETE context."""
        handler = DeleteHandler()

        class MockOtherContext:
            pass

        ctx = MockOtherContext()
        assert handler.can_handle(ctx) is False

    def test_handle_visitor_success(self):
        """Test handle_visitor resets parse result."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult(
            collection="old_collection", filter_conditions={"old": "value"}, has_errors=True, error_message="old error"
        )

        class MockContext:
            pass

        ctx = MockContext()
        result = handler.handle_visitor(ctx, parse_result)

        # Verify reset
        assert result.collection is None
        assert result.filter_conditions == {}
        assert result.has_errors is False
        assert result.error_message is None

    def test_handle_visitor_with_exception(self):
        """Test handle_visitor handles exceptions."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        # Force an exception by passing None
        result = handler.handle_visitor(None, parse_result)

        # Should handle error gracefully
        assert isinstance(result, DeleteParseResult)

    def test_handle_from_clause_explicit_success(self):
        """Test handle_from_clause_explicit extracts collection name."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        # Mock context with pathSimple
        class MockPathSimple:
            def getText(self):
                return "test_collection"

        class MockFromClauseContext:
            def pathSimple(self):
                return MockPathSimple()

        ctx = MockFromClauseContext()
        collection = handler.handle_from_clause_explicit(ctx, parse_result)

        assert collection == "test_collection"
        assert parse_result.collection == "test_collection"
        assert parse_result.has_errors is False

    def test_handle_from_clause_explicit_no_path(self):
        """Test handle_from_clause_explicit when pathSimple returns None."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        class MockFromClauseContext:
            def pathSimple(self):
                return None

        ctx = MockFromClauseContext()
        collection = handler.handle_from_clause_explicit(ctx, parse_result)

        assert collection is None

    def test_handle_from_clause_explicit_with_error(self):
        """Test handle_from_clause_explicit handles exceptions."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        class MockPathSimple:
            def getText(self):
                raise ValueError("Test error")

        class MockFromClauseContext:
            def pathSimple(self):
                return MockPathSimple()

        ctx = MockFromClauseContext()
        collection = handler.handle_from_clause_explicit(ctx, parse_result)

        assert collection is None
        assert parse_result.has_errors is True
        assert "Test error" in parse_result.error_message

    def test_handle_from_clause_implicit_success(self):
        """Test handle_from_clause_implicit extracts collection name."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        class MockPathSimple:
            def getText(self):
                return "implicit_collection"

        class MockFromClauseContext:
            def pathSimple(self):
                return MockPathSimple()

        ctx = MockFromClauseContext()
        collection = handler.handle_from_clause_implicit(ctx, parse_result)

        assert collection == "implicit_collection"
        assert parse_result.collection == "implicit_collection"
        assert parse_result.has_errors is False

    def test_handle_from_clause_implicit_no_path(self):
        """Test handle_from_clause_implicit when pathSimple returns None."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        class MockFromClauseContext:
            def pathSimple(self):
                return None

        ctx = MockFromClauseContext()
        collection = handler.handle_from_clause_implicit(ctx, parse_result)

        assert collection is None

    def test_handle_from_clause_implicit_with_error(self):
        """Test handle_from_clause_implicit handles exceptions."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        class MockPathSimple:
            def getText(self):
                raise RuntimeError("Implicit error")

        class MockFromClauseContext:
            def pathSimple(self):
                return MockPathSimple()

        ctx = MockFromClauseContext()
        collection = handler.handle_from_clause_implicit(ctx, parse_result)

        assert collection is None
        assert parse_result.has_errors is True
        assert "Implicit error" in parse_result.error_message

    def test_handle_where_clause_no_expression(self):
        """Test handle_where_clause when no expression is present."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        class MockWhereContext:
            arg = None

            def expr(self):
                return None

        ctx = MockWhereContext()
        result = handler.handle_where_clause(ctx, parse_result)

        # Should return empty dict (delete all)
        assert result == {}
        assert parse_result.filter_conditions == {}

    def test_handle_where_clause_with_error(self):
        """Test handle_where_clause handles exceptions."""
        handler = DeleteHandler()
        parse_result = DeleteParseResult()

        class MockExpr:
            def getText(self):
                raise Exception("WHERE error")

        class MockWhereContext:
            arg = None

            def expr(self):
                return MockExpr()

        ctx = MockWhereContext()
        result = handler.handle_where_clause(ctx, parse_result)

        assert result == {}
        assert parse_result.has_errors is True
        assert "WHERE error" in parse_result.error_message
