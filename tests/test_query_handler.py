# -*- coding: utf-8 -*-
from pymongosql.sql.query_handler import FromHandler, QueryParseResult, SelectHandler, WhereHandler


class TestQueryParseResult:
    """Test QueryParseResult dataclass."""

    def test_for_visitor_factory(self):
        """Test factory method creates fresh instance."""
        result = QueryParseResult.for_visitor()
        assert result.filter_conditions == {}
        assert result.has_errors is False
        assert result.error_message is None
        assert result.collection is None
        assert result.projection == {}
        assert result.column_aliases == {}
        assert result.sort_fields == []
        assert result.limit_value is None
        assert result.offset_value is None

    def test_merge_expression_with_filters(self):
        """Test merge_expression merges filter conditions."""
        result1 = QueryParseResult(filter_conditions={"age": {"$gt": 18}})
        result2 = QueryParseResult(filter_conditions={"status": "active"})

        result1.merge_expression(result2)

        # Should combine with $and
        assert "$and" in result1.filter_conditions
        assert result1.filter_conditions["$and"] == [{"age": {"$gt": 18}}, {"status": "active"}]

    def test_merge_expression_no_existing_filter(self):
        """Test merge_expression when no existing filter."""
        result1 = QueryParseResult()
        result2 = QueryParseResult(filter_conditions={"status": "active"})

        result1.merge_expression(result2)

        assert result1.filter_conditions == {"status": "active"}

    def test_merge_expression_with_errors(self):
        """Test merge_expression propagates errors."""
        result1 = QueryParseResult()
        result2 = QueryParseResult(has_errors=True, error_message="Test error")

        result1.merge_expression(result2)

        assert result1.has_errors is True
        assert result1.error_message == "Test error"

    def test_mongo_filter_property_getter(self):
        """Test mongo_filter property (backward compatibility)."""
        result = QueryParseResult(filter_conditions={"age": 25})
        assert result.mongo_filter == {"age": 25}

    def test_mongo_filter_property_setter(self):
        """Test mongo_filter property setter (backward compatibility)."""
        result = QueryParseResult()
        result.mongo_filter = {"age": 30}
        assert result.filter_conditions == {"age": 30}


class TestSelectHandler:
    """Test SelectHandler class."""

    def test_can_handle_projection_items(self):
        """Test can_handle returns True for projectionItems context."""
        handler = SelectHandler()

        class MockContext:
            def projectionItems(self):
                return True

        assert handler.can_handle(MockContext()) is True

    def test_can_handle_no_projection_items(self):
        """Test can_handle returns False when no projectionItems."""
        handler = SelectHandler()

        class MockContext:
            pass

        assert handler.can_handle(MockContext()) is False

    def test_extract_field_and_alias_simple_field(self):
        """Test _extract_field_and_alias with simple field."""
        handler = SelectHandler()

        class MockChild:
            def getText(self):
                return "field_name"

        class MockItem:
            children = [MockChild()]

        field_name, alias, func_info = handler._extract_field_and_alias(MockItem())
        assert field_name == "field_name"
        assert alias is None
        assert func_info is None

    def test_extract_field_and_alias_with_as_keyword(self):
        """Test _extract_field_and_alias with AS keyword."""
        handler = SelectHandler()

        class MockField:
            def getText(self):
                return "field_name"

        class MockAS:
            def getText(self):
                return "AS"

        class MockAlias:
            def getText(self):
                return "field_alias"

        class MockItem:
            children = [MockField(), MockAS(), MockAlias()]

        field_name, alias, func_info = handler._extract_field_and_alias(MockItem())
        assert field_name == "field_name"
        assert alias == "field_alias"
        assert func_info is None

    def test_extract_field_and_alias_without_as_keyword(self):
        """Test _extract_field_and_alias without AS keyword."""
        handler = SelectHandler()

        class MockField:
            def getText(self):
                return "field_name"

        class MockAlias:
            def getText(self):
                return "alias_name"

        class MockItem:
            children = [MockField(), MockAlias()]

        field_name, alias, func_info = handler._extract_field_and_alias(MockItem())
        assert field_name == "field_name"
        assert alias == "alias_name"
        assert func_info is None

    def test_extract_field_and_alias_no_children(self):
        """Test _extract_field_and_alias when no children."""
        handler = SelectHandler()

        class MockItem:
            def __str__(self):
                return "simple_item"

        field_name, alias, func_info = handler._extract_field_and_alias(MockItem())
        assert alias is None
        assert func_info is None


class TestFromHandler:
    """Test FromHandler class."""

    def test_can_handle_table_reference(self):
        """Test can_handle returns True for tableReference context."""
        handler = FromHandler()

        class MockContext:
            def tableReference(self):
                return True

        assert handler.can_handle(MockContext()) is True

    def test_can_handle_no_table_reference(self):
        """Test can_handle returns False when no tableReference."""
        handler = FromHandler()

        class MockContext:
            pass

        assert handler.can_handle(MockContext()) is False

    def test_handle_visitor_extracts_collection(self):
        """Test handle_visitor extracts collection name."""
        handler = FromHandler()
        parse_result = QueryParseResult()

        class MockTableRef:
            def getText(self):
                return "test_collection"

        class MockContext:
            def tableReference(self):
                return MockTableRef()

        ctx = MockContext()
        collection = handler.handle_visitor(ctx, parse_result)

        assert collection == "test_collection"
        assert parse_result.collection == "test_collection"

    def test_handle_visitor_no_table_reference(self):
        """Test handle_visitor when no tableReference."""
        handler = FromHandler()
        parse_result = QueryParseResult()

        class MockContext:
            def tableReference(self):
                return None

        ctx = MockContext()
        result = handler.handle_visitor(ctx, parse_result)

        assert result is None


class TestWhereHandler:
    """Test WhereHandler class."""

    def test_can_handle_expr_select(self):
        """Test can_handle returns True for exprSelect context."""
        handler = WhereHandler()

        class MockContext:
            def exprSelect(self):
                return True

        assert handler.can_handle(MockContext()) is True

    def test_can_handle_no_expr_select(self):
        """Test can_handle returns False when no exprSelect."""
        handler = WhereHandler()

        class MockContext:
            pass

        assert handler.can_handle(MockContext()) is False

    def test_handle_visitor_no_expression(self):
        """Test handle_visitor when no exprSelect."""
        handler = WhereHandler()
        parse_result = QueryParseResult()

        class MockContext:
            def exprSelect(self):
                return None

        ctx = MockContext()
        result = handler.handle_visitor(ctx, parse_result)

        assert result == {}

    def test_handle_visitor_with_exception_fallback(self):
        """Test handle_visitor falls back to text search on exception."""
        handler = WhereHandler()
        parse_result = QueryParseResult()

        class MockExpr:
            def getText(self):
                return "field = value"

        class MockContext:
            def exprSelect(self):
                return MockExpr()

        ctx = MockContext()
        result = handler.handle_visitor(ctx, parse_result)

        # Should fallback to text search when expression handler fails
        # The actual behavior depends on expression handler implementation
        assert isinstance(result, dict)
