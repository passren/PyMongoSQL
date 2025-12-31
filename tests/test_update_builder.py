# -*- coding: utf-8 -*-
import pytest

from pymongosql.sql.update_builder import MongoUpdateBuilder, UpdateExecutionPlan


class TestUpdateExecutionPlan:
    """Test UpdateExecutionPlan dataclass."""

    def test_to_dict(self):
        """Test to_dict conversion."""
        plan = UpdateExecutionPlan(
            collection="users", update_fields={"name": "John", "age": 30}, filter_conditions={"id": 123}
        )

        result = plan.to_dict()
        assert result["collection"] == "users"
        assert result["filter"] == {"id": 123}
        assert result["update"] == {"$set": {"name": "John", "age": 30}}

    def test_validate_success(self):
        """Test validate returns True for valid plan."""
        plan = UpdateExecutionPlan(collection="products", update_fields={"price": 99.99})

        assert plan.validate() is True

    def test_validate_no_update_fields(self):
        """Test validate fails when no update fields."""
        plan = UpdateExecutionPlan(collection="products", update_fields={})

        assert plan.validate() is False

    def test_validate_empty_filter_allowed(self):
        """Test validate allows empty filter (update all)."""
        plan = UpdateExecutionPlan(collection="products", update_fields={"status": "active"}, filter_conditions={})

        assert plan.validate() is True

    def test_copy(self):
        """Test copy creates independent copy."""
        original = UpdateExecutionPlan(
            collection="orders", update_fields={"status": "shipped", "total": 100}, filter_conditions={"id": 456}
        )

        copy = original.copy()

        # Verify all fields copied
        assert copy.collection == original.collection
        assert copy.update_fields == original.update_fields
        assert copy.filter_conditions == original.filter_conditions

        # Verify it's independent
        copy.collection = "new_collection"
        copy.update_fields["new_field"] = "value"
        assert original.collection == "orders"
        assert "new_field" not in original.update_fields

    def test_copy_with_empty_fields(self):
        """Test copy handles empty dicts."""
        original = UpdateExecutionPlan(collection="test", update_fields={"field": "value"})

        copy = original.copy()
        assert copy.filter_conditions == {}

    def test_get_mongo_update_doc(self):
        """Test get_mongo_update_doc returns $set document."""
        plan = UpdateExecutionPlan(collection="users", update_fields={"email": "user@example.com", "verified": True})

        update_doc = plan.get_mongo_update_doc()
        assert update_doc == {"$set": {"email": "user@example.com", "verified": True}}

    def test_parameter_style_default(self):
        """Test default parameter style is qmark."""
        plan = UpdateExecutionPlan(collection="test", update_fields={"a": "b"})
        assert plan.parameter_style == "qmark"


class TestMongoUpdateBuilder:
    """Test MongoUpdateBuilder class."""

    def test_collection(self):
        """Test setting collection name."""
        builder = MongoUpdateBuilder()
        result = builder.collection("users")

        assert builder._plan.collection == "users"
        assert result is builder  # Fluent interface

    def test_update_fields(self):
        """Test setting update fields."""
        builder = MongoUpdateBuilder()
        builder.update_fields({"name": "Alice", "age": 25})

        assert builder._plan.update_fields == {"name": "Alice", "age": 25}

    def test_update_fields_empty_dict(self):
        """Test update_fields with empty dict doesn't update."""
        builder = MongoUpdateBuilder()
        builder.update_fields({})

        assert builder._plan.update_fields == {}

    def test_update_fields_none(self):
        """Test update_fields with None doesn't update."""
        builder = MongoUpdateBuilder()
        builder._plan.update_fields = {"existing": "field"}
        builder.update_fields(None)

        # Should preserve existing
        assert builder._plan.update_fields == {"existing": "field"}

    def test_filter_conditions(self):
        """Test setting filter conditions."""
        builder = MongoUpdateBuilder()
        builder.filter_conditions({"status": "active", "age": {"$gt": 18}})

        assert builder._plan.filter_conditions == {"status": "active", "age": {"$gt": 18}}

    def test_filter_conditions_empty(self):
        """Test filter_conditions with empty dict doesn't update."""
        builder = MongoUpdateBuilder()
        builder.filter_conditions({})

        assert builder._plan.filter_conditions == {}

    def test_filter_conditions_none(self):
        """Test filter_conditions with None doesn't update."""
        builder = MongoUpdateBuilder()
        builder._plan.filter_conditions = {"existing": "filter"}
        builder.filter_conditions(None)

        # Should preserve existing
        assert builder._plan.filter_conditions == {"existing": "filter"}

    def test_parameter_style(self):
        """Test setting parameter style."""
        builder = MongoUpdateBuilder()
        builder.parameter_style("named")

        assert builder._plan.parameter_style == "named"

    def test_build_success(self):
        """Test build returns execution plan when valid."""
        builder = MongoUpdateBuilder()
        builder.collection("products").update_fields({"price": 49.99})

        plan = builder.build()

        assert isinstance(plan, UpdateExecutionPlan)
        assert plan.collection == "products"
        assert plan.update_fields == {"price": 49.99}

    def test_build_validation_failure(self):
        """Test build raises ValueError when validation fails."""
        builder = MongoUpdateBuilder()
        builder.collection("products")
        # Don't set update_fields

        with pytest.raises(ValueError) as exc_info:
            builder.build()

        assert "invalid update plan" in str(exc_info.value).lower()

    def test_fluent_interface_chaining(self):
        """Test all methods return self for chaining."""
        builder = MongoUpdateBuilder()

        result = (
            builder.collection("orders")
            .update_fields({"status": "shipped"})
            .filter_conditions({"id": 123})
            .parameter_style("qmark")
        )

        assert result is builder
        assert builder._plan.collection == "orders"
        assert builder._plan.update_fields == {"status": "shipped"}
        assert builder._plan.filter_conditions == {"id": 123}
        assert builder._plan.parameter_style == "qmark"
