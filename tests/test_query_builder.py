# -*- coding: utf-8 -*-
import pytest

from pymongosql.sql.query_builder import MongoQueryBuilder, QueryExecutionPlan


class TestQueryExecutionPlan:
    """Test QueryExecutionPlan dataclass."""

    def test_to_dict(self):
        """Test to_dict conversion."""
        plan = QueryExecutionPlan(
            collection="users",
            filter_stage={"age": {"$gt": 18}},
            projection_stage={"name": 1, "email": 1},
            sort_stage=[{"name": 1}],
            limit_stage=10,
            skip_stage=5,
        )

        result = plan.to_dict()
        assert result["collection"] == "users"
        assert result["filter"] == {"age": {"$gt": 18}}
        assert result["projection"] == {"name": 1, "email": 1}
        assert result["sort"] == [{"name": 1}]
        assert result["limit"] == 10
        assert result["skip"] == 5

    def test_validate_success(self):
        """Test validate returns True for valid plan."""
        plan = QueryExecutionPlan(collection="products", limit_stage=100, skip_stage=0)

        assert plan.validate() is True

    def test_validate_negative_limit(self):
        """Test validate fails for negative limit."""
        plan = QueryExecutionPlan(collection="products", limit_stage=-1)

        assert plan.validate() is False

    def test_validate_invalid_limit_type(self):
        """Test validate fails for non-integer limit."""
        plan = QueryExecutionPlan(collection="products", limit_stage="10")  # String instead of int

        assert plan.validate() is False

    def test_validate_negative_skip(self):
        """Test validate fails for negative skip."""
        plan = QueryExecutionPlan(collection="products", skip_stage=-5)

        assert plan.validate() is False

    def test_validate_invalid_skip_type(self):
        """Test validate fails for non-integer skip."""
        plan = QueryExecutionPlan(collection="products", skip_stage=5.5)  # Float instead of int

        assert plan.validate() is False

    def test_copy(self):
        """Test copy creates independent copy."""
        original = QueryExecutionPlan(
            collection="orders",
            filter_stage={"status": "active"},
            projection_stage={"total": 1},
            column_aliases={"total": "amount"},
            sort_stage=[{"date": -1}],
            limit_stage=50,
            skip_stage=10,
        )

        copy = original.copy()

        # Verify all fields copied
        assert copy.collection == original.collection
        assert copy.filter_stage == original.filter_stage
        assert copy.projection_stage == original.projection_stage
        assert copy.column_aliases == original.column_aliases
        assert copy.sort_stage == original.sort_stage
        assert copy.limit_stage == original.limit_stage
        assert copy.skip_stage == original.skip_stage

        # Verify it's independent (modify copy doesn't affect original)
        copy.collection = "new_collection"
        copy.filter_stage["new_key"] = "new_value"
        assert original.collection == "orders"
        assert "new_key" not in original.filter_stage


class TestMongoQueryBuilder:
    """Test MongoQueryBuilder class."""

    def test_collection_valid(self):
        """Test setting collection name."""
        builder = MongoQueryBuilder()
        result = builder.collection("users")

        assert builder._execution_plan.collection == "users"
        assert result is builder  # Fluent interface

    def test_collection_empty_string(self):
        """Test collection with empty string adds error."""
        builder = MongoQueryBuilder()
        builder.collection("")

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "cannot be empty" in errors[0].lower()

    def test_collection_whitespace_only(self):
        """Test collection with whitespace only adds error."""
        builder = MongoQueryBuilder()
        builder.collection("   ")

        errors = builder.get_errors()
        assert len(errors) > 0

    def test_filter_valid_dict(self):
        """Test filter with valid dictionary."""
        builder = MongoQueryBuilder()
        builder.filter({"age": {"$gt": 25}})

        assert builder._execution_plan.filter_stage == {"age": {"$gt": 25}}

    def test_filter_invalid_type(self):
        """Test filter with non-dict adds error."""
        builder = MongoQueryBuilder()
        builder.filter("not a dict")

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "must be a dictionary" in errors[0].lower()

    def test_filter_multiple_calls(self):
        """Test multiple filter calls update conditions."""
        builder = MongoQueryBuilder()
        builder.filter({"age": {"$gt": 25}})
        builder.filter({"status": "active"})

        assert builder._execution_plan.filter_stage["age"] == {"$gt": 25}
        assert builder._execution_plan.filter_stage["status"] == "active"

    def test_project_with_list(self):
        """Test project with list of field names."""
        builder = MongoQueryBuilder()
        builder.project(["name", "email", "age"])

        expected = {"name": 1, "email": 1, "age": 1}
        assert builder._execution_plan.projection_stage == expected

    def test_project_with_dict(self):
        """Test project with dictionary."""
        builder = MongoQueryBuilder()
        builder.project({"name": 1, "email": 1, "_id": 0})

        assert builder._execution_plan.projection_stage == {"name": 1, "email": 1, "_id": 0}

    def test_project_with_invalid_type(self):
        """Test project with invalid type adds error."""
        builder = MongoQueryBuilder()
        builder.project("name, email")

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "must be a list" in errors[0].lower() or "dictionary" in errors[0].lower()

    def test_sort_valid_specs(self):
        """Test sort with valid specifications."""
        builder = MongoQueryBuilder()
        builder.sort([{"name": 1}, {"age": -1}])

        assert builder._execution_plan.sort_stage == [{"name": 1}, {"age": -1}]

    def test_sort_invalid_type(self):
        """Test sort with non-list adds error."""
        builder = MongoQueryBuilder()
        builder.sort({"name": 1})  # Dict instead of list

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "must be a list" in errors[0].lower()

    def test_sort_invalid_spec_multiple_keys(self):
        """Test sort with multi-key dict adds error."""
        builder = MongoQueryBuilder()
        builder.sort([{"name": 1, "age": -1}])  # Two keys in one dict

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "single-key dict" in errors[0].lower()

    def test_sort_invalid_direction(self):
        """Test sort with invalid direction adds error."""
        builder = MongoQueryBuilder()
        builder.sort([{"name": 2}])  # Direction must be 1 or -1

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "must be 1 or -1" in errors[0].lower()

    def test_sort_empty_field_name(self):
        """Test sort with empty field name adds error."""
        builder = MongoQueryBuilder()
        builder.sort([{"": 1}])

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "non-empty string" in errors[0].lower()

    def test_sort_non_string_field(self):
        """Test sort with non-string field adds error."""
        builder = MongoQueryBuilder()
        builder.sort([{123: 1}])

        errors = builder.get_errors()
        assert len(errors) > 0

    def test_limit_valid(self):
        """Test limit with valid value."""
        builder = MongoQueryBuilder()
        builder.limit(100)

        assert builder._execution_plan.limit_stage == 100

    def test_limit_negative(self):
        """Test limit with negative value adds error."""
        builder = MongoQueryBuilder()
        builder.limit(-10)

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "non-negative" in errors[0].lower()

    def test_limit_non_integer(self):
        """Test limit with non-integer adds error."""
        builder = MongoQueryBuilder()
        builder.limit(10.5)

        errors = builder.get_errors()
        assert len(errors) > 0

    def test_skip_valid(self):
        """Test skip with valid value."""
        builder = MongoQueryBuilder()
        builder.skip(50)

        assert builder._execution_plan.skip_stage == 50

    def test_skip_negative(self):
        """Test skip with negative value adds error."""
        builder = MongoQueryBuilder()
        builder.skip(-5)

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "non-negative" in errors[0].lower()

    def test_skip_non_integer(self):
        """Test skip with non-integer adds error."""
        builder = MongoQueryBuilder()
        builder.skip("10")

        errors = builder.get_errors()
        assert len(errors) > 0

    def test_column_aliases_valid(self):
        """Test column_aliases with valid dict."""
        builder = MongoQueryBuilder()
        builder.column_aliases({"user_name": "name", "user_email": "email"})

        assert builder._execution_plan.column_aliases == {"user_name": "name", "user_email": "email"}

    def test_column_aliases_invalid_type(self):
        """Test column_aliases with non-dict adds error."""
        builder = MongoQueryBuilder()
        builder.column_aliases(["name", "email"])

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "must be a dictionary" in errors[0].lower()

    def test_where_equality(self):
        """Test where with equality operator."""
        builder = MongoQueryBuilder()
        builder.where("status", "=", "active")

        assert builder._execution_plan.filter_stage == {"status": {"$eq": "active"}}

    def test_where_greater_than(self):
        """Test where with greater than operator."""
        builder = MongoQueryBuilder()
        builder.where("age", ">", 18)

        assert builder._execution_plan.filter_stage == {"age": {"$gt": 18}}

    def test_where_less_than_or_equal(self):
        """Test where with less than or equal operator."""
        builder = MongoQueryBuilder()
        builder.where("price", "<=", 100.0)

        assert builder._execution_plan.filter_stage == {"price": {"$lte": 100.0}}

    def test_where_not_equal(self):
        """Test where with not equal operator."""
        builder = MongoQueryBuilder()
        builder.where("status", "!=", "deleted")

        assert builder._execution_plan.filter_stage == {"status": {"$ne": "deleted"}}

    def test_where_unsupported_operator(self):
        """Test where with unsupported operator adds error."""
        builder = MongoQueryBuilder()
        builder.where("field", "INVALID", "value")

        errors = builder.get_errors()
        assert len(errors) > 0
        assert "unsupported operator" in errors[0].lower()

    def test_where_in(self):
        """Test where_in method."""
        builder = MongoQueryBuilder()
        builder.where_in("category", ["books", "music", "movies"])

        assert builder._execution_plan.filter_stage == {"category": {"$in": ["books", "music", "movies"]}}

    def test_where_between(self):
        """Test where_between method."""
        builder = MongoQueryBuilder()
        builder.where_between("age", 18, 65)

        assert builder._execution_plan.filter_stage == {"age": {"$gte": 18, "$lte": 65}}

    def test_where_like(self):
        """Test where_like method converts SQL pattern to regex."""
        builder = MongoQueryBuilder()
        builder.where_like("name", "John%")

        filter_stage = builder._execution_plan.filter_stage
        assert "name" in filter_stage
        assert "$regex" in filter_stage["name"]
        assert filter_stage["name"]["$regex"] == "John.*"
        assert filter_stage["name"]["$options"] == "i"

    def test_where_like_with_underscore(self):
        """Test where_like converts underscore to dot."""
        builder = MongoQueryBuilder()
        builder.where_like("code", "A_C")

        filter_stage = builder._execution_plan.filter_stage
        assert filter_stage["code"]["$regex"] == "A.C"

    def test_validate_success(self):
        """Test validate returns True when collection is set."""
        builder = MongoQueryBuilder()
        builder.collection("users")

        assert builder.validate() is True

    def test_validate_missing_collection(self):
        """Test validate returns False when collection is missing."""
        builder = MongoQueryBuilder()

        assert builder.validate() is False
        errors = builder.get_errors()
        assert len(errors) > 0
        assert "collection name is required" in errors[0].lower()

    def test_build_success(self):
        """Test build returns execution plan when valid."""
        builder = MongoQueryBuilder()
        builder.collection("users").filter({"age": {"$gt": 18}}).limit(10)

        plan = builder.build()

        assert isinstance(plan, QueryExecutionPlan)
        assert plan.collection == "users"
        assert plan.filter_stage == {"age": {"$gt": 18}}
        assert plan.limit_stage == 10

    def test_build_validation_failure(self):
        """Test build raises ValueError when validation fails."""
        builder = MongoQueryBuilder()
        # Don't set collection

        with pytest.raises(ValueError) as exc_info:
            builder.build()

        assert "validation failed" in str(exc_info.value).lower()

    def test_reset(self):
        """Test reset clears builder state."""
        builder = MongoQueryBuilder()
        builder.collection("users").filter({"age": {"$gt": 18}}).limit(10)

        # Add an error
        builder.collection("")

        # Reset
        builder.reset()

        assert builder._execution_plan.collection is None
        assert builder._execution_plan.filter_stage == {}
        assert builder._execution_plan.limit_stage is None
        assert len(builder.get_errors()) == 0

    def test_str_representation(self):
        """Test __str__ method."""
        builder = MongoQueryBuilder()
        builder.collection("products").filter({"price": {"$lt": 100}}).project(["name", "price"])

        str_repr = str(builder)

        assert "MongoQueryBuilder" in str_repr
        assert "collection=products" in str_repr

    def test_fluent_interface_chaining(self):
        """Test all methods return self for chaining."""
        builder = MongoQueryBuilder()

        result = (
            builder.collection("orders")
            .filter({"status": "pending"})
            .project(["id", "total"])
            .sort([{"date": -1}])
            .limit(100)
            .skip(50)
            .column_aliases({"id": "order_id"})
        )

        assert result is builder
        assert builder._execution_plan.collection == "orders"
        assert builder._execution_plan.limit_stage == 100
        assert builder._execution_plan.skip_stage == 50
