# -*- coding: utf-8 -*-
import pytest

from pymongosql.sql.delete_builder import DeleteExecutionPlan, MongoDeleteBuilder


class TestDeleteExecutionPlan:
    """Test DeleteExecutionPlan dataclass."""

    def test_to_dict(self):
        """Test to_dict conversion."""
        plan = DeleteExecutionPlan(collection="users", filter_conditions={"age": {"$lt": 18}})

        result = plan.to_dict()
        assert result["collection"] == "users"
        assert result["filter"] == {"age": {"$lt": 18}}

    def test_to_dict_empty_filter(self):
        """Test to_dict with empty filter."""
        plan = DeleteExecutionPlan(collection="logs", filter_conditions={})

        result = plan.to_dict()
        assert result["collection"] == "logs"
        assert result["filter"] == {}

    def test_validate_success(self):
        """Test validate returns True for valid plan."""
        plan = DeleteExecutionPlan(collection="products", filter_conditions={"status": "inactive"})

        assert plan.validate() is True

    def test_validate_empty_filter_allowed(self):
        """Test validate allows empty filter (delete all)."""
        plan = DeleteExecutionPlan(collection="temp_data", filter_conditions={})

        assert plan.validate() is True

    def test_copy(self):
        """Test copy creates independent copy."""
        original = DeleteExecutionPlan(collection="orders", filter_conditions={"status": "cancelled", "year": 2020})

        copy = original.copy()

        # Verify all fields copied
        assert copy.collection == original.collection
        assert copy.filter_conditions == original.filter_conditions

        # Verify it's independent
        copy.collection = "new_collection"
        copy.filter_conditions["new_field"] = "value"
        assert original.collection == "orders"
        assert "new_field" not in original.filter_conditions

    def test_copy_with_empty_filter(self):
        """Test copy handles empty filter dict."""
        original = DeleteExecutionPlan(collection="test", filter_conditions={})

        copy = original.copy()
        assert copy.filter_conditions == {}

        # Verify it's independent
        copy.filter_conditions["new"] = "value"
        assert original.filter_conditions == {}

    def test_parameter_style_default(self):
        """Test default parameter style is qmark."""
        plan = DeleteExecutionPlan(collection="test")
        assert plan.parameter_style == "qmark"


class TestMongoDeleteBuilder:
    """Test MongoDeleteBuilder class."""

    def test_collection(self):
        """Test setting collection name."""
        builder = MongoDeleteBuilder()
        result = builder.collection("users")

        assert builder._plan.collection == "users"
        assert result is builder  # Fluent interface

    def test_filter_conditions(self):
        """Test setting filter conditions."""
        builder = MongoDeleteBuilder()
        builder.filter_conditions({"status": "deleted", "age": {"$gt": 100}})

        assert builder._plan.filter_conditions == {"status": "deleted", "age": {"$gt": 100}}

    def test_filter_conditions_empty(self):
        """Test filter_conditions with empty dict doesn't update."""
        builder = MongoDeleteBuilder()
        builder.filter_conditions({})

        assert builder._plan.filter_conditions == {}

    def test_filter_conditions_none(self):
        """Test filter_conditions with None doesn't update."""
        builder = MongoDeleteBuilder()
        builder._plan.filter_conditions = {"existing": "filter"}
        builder.filter_conditions(None)

        # Should preserve existing
        assert builder._plan.filter_conditions == {"existing": "filter"}

    def test_build_success(self):
        """Test build returns execution plan when valid."""
        builder = MongoDeleteBuilder()
        builder.collection("products").filter_conditions({"price": {"$lt": 10}})

        plan = builder.build()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "products"
        assert plan.filter_conditions == {"price": {"$lt": 10}}

    def test_build_success_empty_filter(self):
        """Test build succeeds with empty filter (delete all)."""
        builder = MongoDeleteBuilder()
        builder.collection("temp_logs")

        plan = builder.build()

        assert isinstance(plan, DeleteExecutionPlan)
        assert plan.collection == "temp_logs"
        assert plan.filter_conditions == {}

    def test_build_validation_failure(self):
        """Test build raises ValueError when validation fails."""
        builder = MongoDeleteBuilder()
        # Don't set collection

        with pytest.raises(ValueError) as exc_info:
            builder.build()

        assert "invalid delete plan" in str(exc_info.value).lower()

    def test_fluent_interface_chaining(self):
        """Test all methods return self for chaining."""
        builder = MongoDeleteBuilder()

        result = builder.collection("orders").filter_conditions({"status": "expired"})

        assert result is builder
        assert builder._plan.collection == "orders"
        assert builder._plan.filter_conditions == {"status": "expired"}
