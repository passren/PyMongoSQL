# -*- coding: utf-8 -*-
import pytest

from pymongosql.sql.insert_builder import InsertExecutionPlan, MongoInsertBuilder


class TestInsertExecutionPlan:
    """Test InsertExecutionPlan dataclass."""

    def test_to_dict(self):
        """Test to_dict conversion."""
        plan = InsertExecutionPlan(
            collection="users", insert_documents=[{"name": "Alice"}, {"name": "Bob"}], parameter_count=2
        )

        result = plan.to_dict()
        assert result["collection"] == "users"
        assert result["documents"] == [{"name": "Alice"}, {"name": "Bob"}]
        assert result["parameter_count"] == 2

    def test_validate_success(self):
        """Test validate returns True for valid plan."""
        plan = InsertExecutionPlan(collection="products", insert_documents=[{"name": "Product A", "price": 99.99}])

        assert plan.validate() is True

    def test_validate_no_documents(self):
        """Test validate fails when no documents."""
        plan = InsertExecutionPlan(collection="products", insert_documents=[])

        assert plan.validate() is False

    def test_copy(self):
        """Test copy creates independent copy."""
        original = InsertExecutionPlan(
            collection="orders",
            insert_documents=[{"id": 1, "total": 100}, {"id": 2, "total": 200}],
            parameter_style="qmark",
            parameter_count=4,
        )

        copy = original.copy()

        # Verify all fields copied
        assert copy.collection == original.collection
        assert copy.insert_documents == original.insert_documents
        assert copy.parameter_style == original.parameter_style
        assert copy.parameter_count == original.parameter_count

        # Verify it's independent
        copy.collection = "new_collection"
        copy.insert_documents[0]["new_field"] = "value"
        assert original.collection == "orders"
        assert "new_field" not in original.insert_documents[0]


class TestMongoInsertBuilder:
    """Test MongoInsertBuilder class."""

    def test_collection_valid(self):
        """Test setting collection name."""
        builder = MongoInsertBuilder()
        result = builder.collection("users")

        assert builder._execution_plan.collection == "users"
        assert result is builder  # Fluent interface

    def test_collection_empty_string(self):
        """Test collection with empty string adds error."""
        builder = MongoInsertBuilder()
        builder.collection("")

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "cannot be empty" in errors[0].lower()

    def test_collection_whitespace_only(self):
        """Test collection with whitespace only adds error."""
        builder = MongoInsertBuilder()
        builder.collection("   ")

        errors = builder.get_errors()
        assert len(errors) > 0

    def test_collection_strips_whitespace(self):
        """Test collection strips whitespace."""
        builder = MongoInsertBuilder()
        builder.collection("  users  ")

        assert builder._execution_plan.collection == "users"

    def test_insert_documents_valid_list(self):
        """Test insert_documents with valid list."""
        builder = MongoInsertBuilder()
        docs = [{"name": "Alice"}, {"name": "Bob"}]
        builder.insert_documents(docs)

        assert builder._execution_plan.insert_documents == docs

    def test_insert_documents_invalid_type(self):
        """Test insert_documents with non-list adds error."""
        builder = MongoInsertBuilder()
        builder.insert_documents("not a list")

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "must be a list" in errors[0].lower()

    def test_insert_documents_empty_list(self):
        """Test insert_documents with empty list adds error."""
        builder = MongoInsertBuilder()
        builder.insert_documents([])

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "at least one document" in errors[0].lower()

    def test_parameter_style_qmark(self):
        """Test parameter_style with qmark."""
        builder = MongoInsertBuilder()
        builder.parameter_style("qmark")

        assert builder._execution_plan.parameter_style == "qmark"

    def test_parameter_style_named(self):
        """Test parameter_style with named."""
        builder = MongoInsertBuilder()
        builder.parameter_style("named")

        assert builder._execution_plan.parameter_style == "named"

    def test_parameter_style_invalid(self):
        """Test parameter_style with invalid value adds error."""
        builder = MongoInsertBuilder()
        builder.parameter_style("invalid")

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "invalid parameter style" in errors[0].lower()

    def test_parameter_style_none(self):
        """Test parameter_style with None is allowed."""
        builder = MongoInsertBuilder()
        builder.parameter_style(None)

        assert builder._execution_plan.parameter_style is None
        assert len(builder.get_errors()) == 0

    def test_parameter_count_valid(self):
        """Test parameter_count with valid value."""
        builder = MongoInsertBuilder()
        builder.parameter_count(5)

        assert builder._execution_plan.parameter_count == 5

    def test_parameter_count_zero(self):
        """Test parameter_count with zero is allowed."""
        builder = MongoInsertBuilder()
        builder.parameter_count(0)

        assert builder._execution_plan.parameter_count == 0

    def test_parameter_count_negative(self):
        """Test parameter_count with negative value adds error."""
        builder = MongoInsertBuilder()
        builder.parameter_count(-1)

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "non-negative" in errors[0].lower()

    def test_parameter_count_non_integer(self):
        """Test parameter_count with non-integer adds error."""
        builder = MongoInsertBuilder()
        builder.parameter_count(5.5)

        errors = builder.get_errors()
        assert len(errors) > 0

    def test_validate_success(self):
        """Test validate returns True when valid."""
        builder = MongoInsertBuilder()
        builder.collection("users").insert_documents([{"name": "Alice"}])

        assert builder.validate() is True

    def test_validate_missing_collection(self):
        """Test validate returns False when collection missing."""
        builder = MongoInsertBuilder()
        builder.insert_documents([{"name": "Alice"}])

        assert builder.validate() is False
        errors = builder.get_errors()
        assert "collection name is required" in errors[0].lower()

    def test_validate_missing_documents(self):
        """Test validate returns False when documents missing."""
        builder = MongoInsertBuilder()
        builder.collection("users")

        assert builder.validate() is False
        errors = builder.get_errors()
        assert "at least one document" in errors[0].lower()

    def test_build_success(self):
        """Test build returns execution plan when valid."""
        builder = MongoInsertBuilder()
        builder.collection("products").insert_documents([{"name": "Product A"}])

        plan = builder.build()

        assert isinstance(plan, InsertExecutionPlan)
        assert plan.collection == "products"
        assert plan.insert_documents == [{"name": "Product A"}]

    def test_build_validation_failure(self):
        """Test build raises ValueError when validation fails."""
        builder = MongoInsertBuilder()
        # Don't set collection or documents

        with pytest.raises(ValueError) as exc_info:
            builder.build()

        assert "validation failed" in str(exc_info.value).lower()

    def test_reset(self):
        """Test reset clears builder state."""
        builder = MongoInsertBuilder()
        builder.collection("users").insert_documents([{"name": "Alice"}]).parameter_count(3)

        # Add an error
        builder.collection("")

        # Reset
        builder.reset()

        assert builder._execution_plan.collection is None
        assert builder._execution_plan.insert_documents == []
        assert builder._execution_plan.parameter_count == 0
        assert len(builder.get_errors()) == 0

    def test_str_representation(self):
        """Test __str__ method."""
        builder = MongoInsertBuilder()
        builder.collection("products").insert_documents([{"a": 1}, {"b": 2}])

        str_repr = str(builder)

        assert "MongoInsertBuilder" in str_repr
        assert "collection=products" in str_repr
        assert "documents=2" in str_repr

    def test_fluent_interface_chaining(self):
        """Test all methods return self for chaining."""
        builder = MongoInsertBuilder()

        result = (
            builder.collection("orders")
            .insert_documents([{"id": 1}, {"id": 2}])
            .parameter_style("qmark")
            .parameter_count(4)
        )

        assert result is builder
        assert builder._execution_plan.collection == "orders"
        assert len(builder._execution_plan.insert_documents) == 2
        assert builder._execution_plan.parameter_style == "qmark"
        assert builder._execution_plan.parameter_count == 4
