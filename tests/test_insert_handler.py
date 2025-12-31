# -*- coding: utf-8 -*-
import pytest

from pymongosql.sql.insert_handler import InsertHandler, InsertParseResult


class TestInsertParseResult:
    """Test InsertParseResult dataclass."""

    def test_for_visitor_factory(self):
        """Test factory method creates fresh instance."""
        result = InsertParseResult.for_visitor()
        assert result.collection is None
        assert result.insert_columns is None
        assert result.insert_values is None
        assert result.insert_documents is None
        assert result.insert_type is None
        assert result.parameter_style is None
        assert result.parameter_count == 0
        assert result.has_errors is False
        assert result.error_message is None


class TestInsertHandler:
    """Test InsertHandler class."""

    def test_can_handle_insert_context(self):
        """Test can_handle returns True for INSERT context."""
        handler = InsertHandler()

        class MockInsertContext:
            def INSERT(self):
                return True

        ctx = MockInsertContext()
        assert handler.can_handle(ctx) is True

    def test_can_handle_non_insert_context(self):
        """Test can_handle returns False for non-INSERT context."""
        handler = InsertHandler()

        class MockOtherContext:
            pass

        ctx = MockOtherContext()
        assert handler.can_handle(ctx) is False

    def test_extract_collection_with_symbol_primitive(self):
        """Test _extract_collection with symbolPrimitive."""
        handler = InsertHandler()

        class MockSymbol:
            def getText(self):
                return "test_collection"

        class MockContext:
            def symbolPrimitive(self):
                return MockSymbol()

        ctx = MockContext()
        collection = handler._extract_collection(ctx)
        assert collection == "test_collection"

    def test_extract_collection_with_path_simple(self):
        """Test _extract_collection with pathSimple (legacy)."""
        handler = InsertHandler()

        class MockPath:
            def getText(self):
                return "legacy_collection"

        class MockContext:
            def symbolPrimitive(self):
                return None

            def pathSimple(self):
                return MockPath()

        ctx = MockContext()
        collection = handler._extract_collection(ctx)
        assert collection == "legacy_collection"

    def test_extract_collection_missing(self):
        """Test _extract_collection raises when collection missing."""
        handler = InsertHandler()

        class MockContext:
            def symbolPrimitive(self):
                return None

            def pathSimple(self):
                return None

        ctx = MockContext()
        with pytest.raises(ValueError) as exc_info:
            handler._extract_collection(ctx)

        assert "missing collection name" in str(exc_info.value).lower()

    def test_parse_expression_value_null(self):
        """Test _parse_expression_value with NULL."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "NULL"

        value = handler._parse_expression_value(MockExpr())
        assert value is None

    def test_parse_expression_value_boolean_true(self):
        """Test _parse_expression_value with TRUE."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "TRUE"

        value = handler._parse_expression_value(MockExpr())
        assert value is True

    def test_parse_expression_value_boolean_false(self):
        """Test _parse_expression_value with FALSE."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "FALSE"

        value = handler._parse_expression_value(MockExpr())
        assert value is False

    def test_parse_expression_value_string_single_quote(self):
        """Test _parse_expression_value with single-quoted string."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "'hello'"

        value = handler._parse_expression_value(MockExpr())
        assert value == "hello"

    def test_parse_expression_value_string_double_quote(self):
        """Test _parse_expression_value with double-quoted string."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return '"world"'

        value = handler._parse_expression_value(MockExpr())
        assert value == "world"

    def test_parse_expression_value_integer(self):
        """Test _parse_expression_value with integer."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "42"

        value = handler._parse_expression_value(MockExpr())
        assert value == 42

    def test_parse_expression_value_float(self):
        """Test _parse_expression_value with float."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "3.14"

        value = handler._parse_expression_value(MockExpr())
        assert value == 3.14

    def test_parse_expression_value_qmark(self):
        """Test _parse_expression_value with ? parameter."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "?"

        value = handler._parse_expression_value(MockExpr())
        assert value == "?"

    def test_parse_expression_value_named_param(self):
        """Test _parse_expression_value with :name parameter."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return ":name"

        value = handler._parse_expression_value(MockExpr())
        assert value == ":name"

    def test_parse_expression_value_none(self):
        """Test _parse_expression_value with None."""
        handler = InsertHandler()
        value = handler._parse_expression_value(None)
        assert value is None

    def test_parse_expression_value_complex(self):
        """Test _parse_expression_value with complex expression."""
        handler = InsertHandler()

        class MockExpr:
            def getText(self):
                return "COMPLEX_EXPR"

        value = handler._parse_expression_value(MockExpr())
        # Complex expressions are returned as-is
        assert value == "COMPLEX_EXPR"

    def test_convert_rows_to_documents_with_columns(self):
        """Test _convert_rows_to_documents with explicit columns."""
        handler = InsertHandler()
        columns = ["name", "age", "city"]
        rows = [["Alice", 25, "NYC"], ["Bob", 30, "LA"]]

        docs = handler._convert_rows_to_documents(columns, rows)

        assert len(docs) == 2
        assert docs[0] == {"name": "Alice", "age": 25, "city": "NYC"}
        assert docs[1] == {"name": "Bob", "age": 30, "city": "LA"}

    def test_convert_rows_to_documents_without_columns(self):
        """Test _convert_rows_to_documents without explicit columns."""
        handler = InsertHandler()
        rows = [["value1", "value2"], ["value3", "value4"]]

        docs = handler._convert_rows_to_documents(None, rows)

        assert len(docs) == 2
        assert docs[0] == {"col0": "value1", "col1": "value2"}
        assert docs[1] == {"col0": "value3", "col1": "value4"}

    def test_convert_rows_to_documents_column_count_mismatch(self):
        """Test _convert_rows_to_documents with column count mismatch."""
        handler = InsertHandler()
        columns = ["name", "age"]
        rows = [["Alice", 25, "Extra"]]  # Too many values

        with pytest.raises(ValueError) as exc_info:
            handler._convert_rows_to_documents(columns, rows)

        assert "column count" in str(exc_info.value).lower()
        assert "value count" in str(exc_info.value).lower()

    def test_normalize_literals(self):
        """Test _normalize_literals replaces PartiQL booleans/null."""
        handler = InsertHandler()

        # Test null variations
        assert "None" in handler._normalize_literals("null")
        assert "None" in handler._normalize_literals("NULL")

        # Test boolean variations
        assert "True" in handler._normalize_literals("true")
        assert "True" in handler._normalize_literals("TRUE")
        assert "False" in handler._normalize_literals("false")
        assert "False" in handler._normalize_literals("FALSE")

    def test_parse_literal_dict_valid(self):
        """Test _parse_literal_dict with valid dict."""
        handler = InsertHandler()
        text = "{'name': 'Alice', 'age': 30}"

        doc = handler._parse_literal_dict(text)
        assert doc == {"name": "Alice", "age": 30}

    def test_parse_literal_dict_invalid(self):
        """Test _parse_literal_dict with invalid dict."""
        handler = InsertHandler()
        text = "not a dict"

        with pytest.raises(ValueError) as exc_info:
            handler._parse_literal_dict(text)

        assert "failed to parse" in str(exc_info.value).lower()

    def test_parse_literal_dict_non_dict_value(self):
        """Test _parse_literal_dict when value is not a dict."""
        handler = InsertHandler()
        text = "['list', 'not', 'dict']"

        with pytest.raises(ValueError) as exc_info:
            handler._parse_literal_dict(text)

        assert "must be an object" in str(exc_info.value).lower()

    def test_parse_literal_list_valid(self):
        """Test _parse_literal_list with valid list of dicts."""
        handler = InsertHandler()
        text = "[{'name': 'Alice'}, {'name': 'Bob'}]"

        docs = handler._parse_literal_list(text)
        assert len(docs) == 2
        assert docs[0] == {"name": "Alice"}
        assert docs[1] == {"name": "Bob"}

    def test_parse_literal_list_invalid(self):
        """Test _parse_literal_list with invalid syntax."""
        handler = InsertHandler()
        text = "not valid"

        with pytest.raises(ValueError) as exc_info:
            handler._parse_literal_list(text)

        assert "failed to parse" in str(exc_info.value).lower()

    def test_parse_literal_list_non_dict_items(self):
        """Test _parse_literal_list when items are not dicts."""
        handler = InsertHandler()
        text = "['string1', 'string2']"

        with pytest.raises(ValueError) as exc_info:
            handler._parse_literal_list(text)

        assert "must contain objects" in str(exc_info.value).lower()

    def test_detect_parameter_style_qmark(self):
        """Test _detect_parameter_style with qmark parameters."""
        handler = InsertHandler()
        docs = [{"name": "?", "age": "?"}]

        style, count = handler._detect_parameter_style(docs)
        assert style == "qmark"
        assert count == 2

    def test_detect_parameter_style_named(self):
        """Test _detect_parameter_style with named parameters."""
        handler = InsertHandler()
        docs = [{"name": ":name", "age": ":age"}]

        style, count = handler._detect_parameter_style(docs)
        assert style == "named"
        assert count == 2

    def test_detect_parameter_style_none(self):
        """Test _detect_parameter_style with no parameters."""
        handler = InsertHandler()
        docs = [{"name": "Alice", "age": 30}]

        style, count = handler._detect_parameter_style(docs)
        assert style is None
        assert count == 0

    def test_detect_parameter_style_mixed_error(self):
        """Test _detect_parameter_style raises on mixed styles."""
        handler = InsertHandler()
        docs = [{"name": "?", "age": ":age"}]  # Mixed qmark and named

        with pytest.raises(ValueError) as exc_info:
            handler._detect_parameter_style(docs)

        assert "mixed parameter styles" in str(exc_info.value).lower()
