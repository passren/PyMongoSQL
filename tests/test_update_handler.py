# -*- coding: utf-8 -*-
from pymongosql.sql.update_handler import UpdateHandler, UpdateParseResult


class TestUpdateParseResult:
    """Test UpdateParseResult dataclass."""

    def test_for_visitor_factory(self):
        """Test factory method creates fresh instance."""
        result = UpdateParseResult.for_visitor()
        assert result.collection is None
        assert result.update_fields == {}
        assert result.filter_conditions == {}
        assert result.has_errors is False
        assert result.error_message is None

    def test_validate_missing_collection(self):
        """Test validation fails when collection is missing."""
        result = UpdateParseResult(update_fields={"name": "value"})
        is_valid = result.validate()

        assert is_valid is False
        assert result.has_errors is True
        assert result.error_message == "Collection name is required"

    def test_validate_missing_update_fields(self):
        """Test validation fails when update fields are missing."""
        result = UpdateParseResult(collection="test_collection")
        is_valid = result.validate()

        assert is_valid is False
        assert result.has_errors is True
        assert result.error_message == "At least one field to update is required"

    def test_validate_success(self):
        """Test validation passes when all required fields set."""
        result = UpdateParseResult(collection="users", update_fields={"name": "John"})
        is_valid = result.validate()

        assert is_valid is True
        assert result.has_errors is False

    def test_to_dict(self):
        """Test to_dict conversion."""
        result = UpdateParseResult(
            collection="users",
            update_fields={"age": 30, "status": "active"},
            filter_conditions={"id": 123},
            has_errors=False,
            error_message=None,
        )

        result_dict = result.to_dict()
        assert result_dict["collection"] == "users"
        assert result_dict["update_fields"] == {"age": 30, "status": "active"}
        assert result_dict["filter_conditions"] == {"id": 123}
        assert result_dict["has_errors"] is False
        assert result_dict["error_message"] is None

    def test_repr(self):
        """Test string representation."""
        result = UpdateParseResult(
            collection="products", update_fields={"price": 99.99}, filter_conditions={"sku": "ABC123"}, has_errors=False
        )

        repr_str = repr(result)
        assert "UpdateParseResult" in repr_str
        assert "collection=products" in repr_str
        assert "has_errors=False" in repr_str


class TestUpdateHandler:
    """Test UpdateHandler class."""

    def test_can_handle_update_context(self):
        """Test can_handle returns True for UPDATE context."""
        handler = UpdateHandler()

        class MockUpdateContext:
            def UPDATE(self):
                return True

        ctx = MockUpdateContext()
        assert handler.can_handle(ctx) is True

    def test_can_handle_non_update_context(self):
        """Test can_handle returns False for non-UPDATE context."""
        handler = UpdateHandler()

        class MockOtherContext:
            pass

        ctx = MockOtherContext()
        assert handler.can_handle(ctx) is False

    def test_handle_visitor_with_table_reference(self):
        """Test handle_visitor extracts collection from tableBaseReference."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockSource:
            def getText(self):
                return "test_collection"

        class MockTableRef:
            source = MockSource()

        class MockContext:
            def tableBaseReference(self):
                return MockTableRef()

        ctx = MockContext()
        result = handler.handle_visitor(ctx, parse_result)

        assert result.collection == "test_collection"
        assert result.has_errors is False

    def test_handle_visitor_without_table_reference(self):
        """Test handle_visitor when no tableBaseReference present."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockContext:
            def tableBaseReference(self):
                return None

        ctx = MockContext()
        result = handler.handle_visitor(ctx, parse_result)

        # Should not set collection
        assert result.collection is None

    def test_handle_visitor_with_error(self):
        """Test handle_visitor logs warning on exception but continues."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockTableRef:
            def __getattribute__(self, name):
                raise RuntimeError("Test error")

        class MockContext:
            def tableBaseReference(self):
                return MockTableRef()

        ctx = MockContext()
        result = handler.handle_visitor(ctx, parse_result)

        # The handler logs warning but doesn't set error flag
        assert result.collection is None

    def test_extract_collection_from_table_ref_with_fallback(self):
        """Test _extract_collection_from_table_ref uses getText fallback."""
        handler = UpdateHandler()

        class MockTableRef:
            def getText(self):
                return "fallback_collection"

        ctx = MockTableRef()
        collection = handler._extract_collection_from_table_ref(ctx)

        assert collection == "fallback_collection"

    def test_extract_collection_with_exception(self):
        """Test _extract_collection_from_table_ref handles exceptions."""
        handler = UpdateHandler()

        class MockTableRef:
            def getText(self):
                raise ValueError("Error extracting")

        ctx = MockTableRef()
        collection = handler._extract_collection_from_table_ref(ctx)

        assert collection is None

    def test_handle_set_command_single_assignment(self):
        """Test handle_set_command with single assignment."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockPathSimple:
            def getText(self):
                return "field_name"

        class MockExpr:
            def getText(self):
                return "'field_value'"

        class MockAssignment:
            def pathSimple(self):
                return MockPathSimple()

            def expr(self):
                return MockExpr()

        class MockContext:
            def setAssignment(self):
                return [MockAssignment()]

        ctx = MockContext()
        result = handler.handle_set_command(ctx, parse_result)

        assert result.update_fields == {"field_name": "field_value"}
        assert result.has_errors is False

    def test_handle_set_command_multiple_assignments(self):
        """Test handle_set_command with multiple assignments."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockAssignment1:
            def pathSimple(self):
                class P:
                    def getText(self):
                        return "name"

                return P()

            def expr(self):
                class E:
                    def getText(self):
                        return "'Alice'"

                return E()

        class MockAssignment2:
            def pathSimple(self):
                class P:
                    def getText(self):
                        return "age"

                return P()

            def expr(self):
                class E:
                    def getText(self):
                        return "30"

                return E()

        class MockContext:
            def setAssignment(self):
                return [MockAssignment1(), MockAssignment2()]

        ctx = MockContext()
        result = handler.handle_set_command(ctx, parse_result)

        assert result.update_fields == {"name": "Alice", "age": 30}

    def test_handle_set_command_with_error(self):
        """Test handle_set_command handles exceptions."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockContext:
            def setAssignment(self):
                raise RuntimeError("SET error")

        ctx = MockContext()
        result = handler.handle_set_command(ctx, parse_result)

        assert result.has_errors is True
        assert "SET error" in result.error_message

    def test_parse_value_string_single_quote(self):
        """Test _parse_value with single-quoted string."""
        handler = UpdateHandler()
        value = handler._parse_value("'hello world'")
        assert value == "hello world"

    def test_parse_value_string_double_quote(self):
        """Test _parse_value with double-quoted string."""
        handler = UpdateHandler()
        value = handler._parse_value('"hello world"')
        assert value == "hello world"

    def test_parse_value_null(self):
        """Test _parse_value with null."""
        handler = UpdateHandler()
        assert handler._parse_value("null") is None
        assert handler._parse_value("NULL") is None

    def test_parse_value_boolean(self):
        """Test _parse_value with booleans."""
        handler = UpdateHandler()
        assert handler._parse_value("true") is True
        assert handler._parse_value("TRUE") is True
        assert handler._parse_value("false") is False
        assert handler._parse_value("FALSE") is False

    def test_parse_value_integer(self):
        """Test _parse_value with integer."""
        handler = UpdateHandler()
        assert handler._parse_value("42") == 42
        assert handler._parse_value("-10") == -10

    def test_parse_value_float(self):
        """Test _parse_value with float."""
        handler = UpdateHandler()
        assert handler._parse_value("3.14") == 3.14
        assert handler._parse_value("-2.5") == -2.5

    def test_parse_value_parameter_qmark(self):
        """Test _parse_value with qmark parameter."""
        handler = UpdateHandler()
        assert handler._parse_value("?") == "?"

    def test_parse_value_parameter_named(self):
        """Test _parse_value with named parameter."""
        handler = UpdateHandler()
        assert handler._parse_value(":name") == ":name"

    def test_parse_value_unquoted_string(self):
        """Test _parse_value with unquoted string."""
        handler = UpdateHandler()
        value = handler._parse_value("unquoted")
        assert value == "unquoted"

    def test_handle_where_clause_no_expression(self):
        """Test handle_where_clause when no expression present."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockWhereContext:
            arg = None

            def expr(self):
                return None

        ctx = MockWhereContext()
        result = handler.handle_where_clause(ctx, parse_result)

        # Should return empty dict (update all)
        assert result == {}
        assert parse_result.filter_conditions == {}

    def test_handle_where_clause_with_error(self):
        """Test handle_where_clause logs error but returns empty dict."""
        handler = UpdateHandler()
        parse_result = UpdateParseResult()

        class MockExpr:
            def getText(self):
                raise Exception("WHERE error")

        class MockWhereContext:
            arg = None

            def expr(self):
                return MockExpr()

        ctx = MockWhereContext()
        result = handler.handle_where_clause(ctx, parse_result)

        # Returns empty dict on error
        assert result == {}
