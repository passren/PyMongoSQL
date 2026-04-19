# -*- coding: utf-8 -*-
"""Tests for CREATE VIEW and DROP VIEW DDL statements."""
import json

import pytest

from pymongosql.error import SqlSyntaxError
from pymongosql.executor import ExecutionContext, ExecutionPlanFactory, ViewExecution
from pymongosql.sql.view_builder import ViewExecutionPlan


class TestDDLParserUnit:
    """Unit tests for DDL SQL parsing."""

    def setup_method(self):
        self.strategy = ViewExecution()

    def test_supports_create_view(self):
        ctx = ExecutionContext("CREATE VIEW my_view ON users AS '[]'")
        assert self.strategy.supports(ctx)

    def test_supports_create_view_case_insensitive(self):
        ctx = ExecutionContext("create view my_view ON users AS '[]'")
        assert self.strategy.supports(ctx)

    def test_supports_drop_view(self):
        ctx = ExecutionContext("DROP VIEW my_view")
        assert self.strategy.supports(ctx)

    def test_does_not_support_select(self):
        ctx = ExecutionContext("SELECT * FROM users")
        assert not self.strategy.supports(ctx)

    def test_does_not_support_create_table(self):
        ctx = ExecutionContext("CREATE TABLE foo (id INT)")
        assert not self.strategy.supports(ctx)

    def test_parse_create_view_simple(self):
        plan = self.strategy._parse_sql('CREATE VIEW filtered_users ON users AS \'[{"$match": {"status": "active"}}]\'')
        assert isinstance(plan, ViewExecutionPlan)
        assert plan.ddl_type == "create_view"
        assert plan.collection == "filtered_users"
        assert plan.view_on == "users"
        assert plan.pipeline == [{"$match": {"status": "active"}}]
        assert plan.validate()

    def test_parse_create_view_empty_pipeline(self):
        plan = self.strategy._parse_sql("CREATE VIEW every_user ON users AS '[]'")
        assert plan.ddl_type == "create_view"
        assert plan.collection == "every_user"
        assert plan.view_on == "users"
        assert plan.pipeline == []
        assert plan.validate()

    def test_parse_create_view_complex_pipeline(self):
        pipeline = [
            {"$lookup": {"from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"}},
            {"$match": {"status": "active"}},
        ]
        plan = self.strategy._parse_sql(f"CREATE VIEW joined_user_orders ON users AS '{json.dumps(pipeline)}'")
        assert plan.pipeline == pipeline

    def test_parse_create_view_invalid_json(self):
        with pytest.raises(SqlSyntaxError, match="Invalid pipeline JSON"):
            self.strategy._parse_sql("CREATE VIEW v ON c AS 'not json'")

    def test_parse_create_view_pipeline_not_array(self):
        with pytest.raises(SqlSyntaxError, match="Pipeline must be a JSON array"):
            self.strategy._parse_sql("CREATE VIEW v ON c AS '{\"$match\": {}}'")

    def test_parse_drop_view(self):
        plan = self.strategy._parse_sql("DROP VIEW my_view")
        assert isinstance(plan, ViewExecutionPlan)
        assert plan.ddl_type == "drop_view"
        assert plan.collection == "my_view"
        assert plan.validate()

    def test_parse_unsupported_ddl(self):
        with pytest.raises(SqlSyntaxError, match="Unsupported DDL"):
            self.strategy._parse_sql("CREATE VIEW")

    def test_factory_selects_ddl_strategy(self):
        ctx = ExecutionContext("CREATE VIEW v ON c AS '[]'")
        strategy = ExecutionPlanFactory.get_strategy(ctx)
        assert isinstance(strategy, ViewExecution)

    def test_factory_selects_ddl_for_drop(self):
        ctx = ExecutionContext("DROP VIEW v")
        strategy = ExecutionPlanFactory.get_strategy(ctx)
        assert isinstance(strategy, ViewExecution)

    def test_plan_validation_missing_collection(self):
        plan = ViewExecutionPlan(ddl_type="create_view", view_on="users", pipeline=[])
        assert not plan.validate()

    def test_plan_validation_missing_view_on(self):
        plan = ViewExecutionPlan(collection="v", ddl_type="create_view", pipeline=[])
        assert not plan.validate()

    def test_plan_to_dict_create(self):
        plan = ViewExecutionPlan(collection="v", ddl_type="create_view", view_on="users", pipeline=[{"$match": {}}])
        d = plan.to_dict()
        assert d["ddl_type"] == "create_view"
        assert d["collection"] == "v"
        assert d["view_on"] == "users"
        assert d["pipeline"] == [{"$match": {}}]

    def test_plan_to_dict_drop(self):
        plan = ViewExecutionPlan(collection="v", ddl_type="drop_view")
        d = plan.to_dict()
        assert d["ddl_type"] == "drop_view"
        assert "view_on" not in d


# ---------------------------------------------------------------------------
# Integration tests – require a running MongoDB instance
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDDLIntegration:
    """Integration tests for CREATE VIEW / DROP VIEW against MongoDB."""

    VIEW_NAME = "test_ddl_view"

    @pytest.fixture(autouse=True)
    def _cleanup_view(self, conn):
        """Ensure the test view does not exist before and after each test."""
        db = conn.database
        if self.VIEW_NAME in db.list_collection_names():
            db.drop_collection(self.VIEW_NAME)
        yield
        if self.VIEW_NAME in db.list_collection_names():
            db.drop_collection(self.VIEW_NAME)

    def test_create_view_and_query(self, conn):
        cursor = conn.cursor()

        pipeline = json.dumps([{"$match": {"active": True}}])
        cursor.execute(f"CREATE VIEW {self.VIEW_NAME} ON users AS '{pipeline}'")

        # The view should now exist
        view_names = conn.database.list_collection_names(filter={"type": "view"})
        assert self.VIEW_NAME in view_names

        # Query the view like a regular collection
        cursor.execute(f"SELECT * FROM {self.VIEW_NAME}")
        rows = cursor.fetchall()
        assert len(rows) > 0

    def test_create_view_with_projection_pipeline(self, conn):
        cursor = conn.cursor()

        pipeline = json.dumps([{"$project": {"name": 1, "email": 1, "_id": 0}}])
        cursor.execute(f"CREATE VIEW {self.VIEW_NAME} ON users AS '{pipeline}'")

        cursor.execute(f"SELECT * FROM {self.VIEW_NAME}")
        rows = cursor.fetchall()
        assert len(rows) > 0

    def test_drop_view(self, conn):
        db = conn.database
        # First create via MongoDB directly
        db.command(
            {
                "create": self.VIEW_NAME,
                "viewOn": "users",
                "pipeline": [],
            }
        )
        assert self.VIEW_NAME in db.list_collection_names()

        cursor = conn.cursor()
        cursor.execute(f"DROP VIEW {self.VIEW_NAME}")

        assert self.VIEW_NAME not in db.list_collection_names()

    def test_create_view_with_lookup(self, conn):
        cursor = conn.cursor()

        pipeline = json.dumps(
            [
                {
                    "$lookup": {
                        "from": "orders",
                        "localField": "_id",
                        "foreignField": "user_id",
                        "as": "user_orders",
                    }
                }
            ]
        )
        cursor.execute(f"CREATE VIEW {self.VIEW_NAME} ON users AS '{pipeline}'")

        cursor.execute(f"SELECT * FROM {self.VIEW_NAME}")
        rows = cursor.fetchall()
        assert len(rows) > 0

    def test_drop_nonexistent_view_succeeds_silently(self, conn):
        """MongoDB drop on a non-existent namespace may succeed silently or raise."""
        cursor = conn.cursor()
        # MongoDB 4.x+ returns ok:1 even when namespace doesn't exist.
        # Just verify it doesn't crash unexpectedly.
        cursor.execute("DROP VIEW nonexistent_view_xyz")

    def test_create_view_roundtrip(self, conn):
        """CREATE VIEW -> query -> DROP VIEW -> confirm gone."""
        cursor = conn.cursor()

        pipeline = json.dumps([{"$match": {"active": True}}])
        cursor.execute(f"CREATE VIEW {self.VIEW_NAME} ON users AS '{pipeline}'")

        cursor.execute(f"SELECT * FROM {self.VIEW_NAME}")
        rows = cursor.fetchall()
        assert len(rows) > 0

        cursor.execute(f"DROP VIEW {self.VIEW_NAME}")
        assert self.VIEW_NAME not in conn.database.list_collection_names()
