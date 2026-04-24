# -*- coding: utf-8 -*-
import json

import pytest

from pymongosql.error import SqlSyntaxError
from pymongosql.executor import (
    ExecutionContext,
    ExecutionPlanFactory,
    ExplainExecution,
    StandardQueryExecution,
)
from pymongosql.sql.explain_builder import ExplainExecutionPlan


class TestExplainParserUnit:
    """Unit tests for EXPLAIN routing and rendering (no MongoDB required)."""

    def setup_method(self):
        self.strategy = ExplainExecution()

    def test_supports_uppercase(self):
        assert self.strategy.supports(ExecutionContext("EXPLAIN SELECT * FROM users"))

    def test_supports_lowercase(self):
        assert self.strategy.supports(ExecutionContext("explain select * from users"))

    def test_supports_leading_whitespace(self):
        assert self.strategy.supports(ExecutionContext("   EXPLAIN\n  SELECT 1"))

    def test_does_not_support_plain_select(self):
        assert not self.strategy.supports(ExecutionContext("SELECT * FROM users"))

    def test_does_not_support_explain_substring(self):
        # Must be a leading keyword, not just present in the query.
        assert not self.strategy.supports(ExecutionContext("SELECT 'EXPLAIN' FROM users"))

    def test_factory_routes_to_explain(self):
        strategy = ExecutionPlanFactory.get_strategy(ExecutionContext("EXPLAIN SELECT * FROM users"))
        assert isinstance(strategy, ExplainExecution)

    def test_factory_still_routes_plain_select(self):
        strategy = ExecutionPlanFactory.get_strategy(ExecutionContext("SELECT * FROM users"))
        assert isinstance(strategy, StandardQueryExecution)


class TestExplainFlatten:
    """Unit tests for the explain-tree flattening / table formatting."""

    def test_flatten_find_with_winning_plan(self):
        explain_result = {
            "queryPlanner": {
                "namespace": "test_db.users",
                "parsedQuery": {"age": {"$gt": 25}},
                "winningPlan": {
                    "stage": "LIMIT",
                    "limitAmount": 10,
                    "inputStage": {
                        "stage": "FETCH",
                        "inputStage": {
                            "stage": "IXSCAN",
                            "indexName": "age_1",
                            "direction": "forward",
                        },
                    },
                },
                "rejectedPlans": [{"dummy": 1}, {"dummy": 2}],
            },
        }
        rows = ExplainExecutionPlan.flatten_result(explain_result)
        stages = [r["stage"] for r in rows]

        assert "namespace" in stages
        assert "parsedQuery" in stages
        assert "rejectedPlans" in stages
        # Tree structure (root has no tree marker, children use └─ / ├─)
        assert "LIMIT" in stages
        assert any(s.endswith("FETCH") and "└─" in s for s in stages)
        assert any(s.endswith("IXSCAN") and "└─" in s for s in stages)

        # rejectedPlans row reports count
        rejected_row = next(r for r in rows if r["stage"] == "rejectedPlans")
        assert rejected_row["details"] == "2"

        # IXSCAN details should carry indexName / direction as JSON
        ixscan_row = next(r for r in rows if r["stage"].endswith("IXSCAN"))
        parsed = json.loads(ixscan_row["details"])
        assert parsed["indexName"] == "age_1"
        assert parsed["direction"] == "forward"

    def test_flatten_multiple_input_stages(self):
        explain_result = {
            "queryPlanner": {
                "namespace": "db.coll",
                "winningPlan": {
                    "stage": "OR",
                    "inputStages": [
                        {"stage": "IXSCAN", "indexName": "a_1"},
                        {"stage": "IXSCAN", "indexName": "b_1"},
                    ],
                },
            },
        }
        rows = ExplainExecutionPlan.flatten_result(explain_result)
        stages = [r["stage"] for r in rows]
        # First branch uses ├─, last uses └─
        assert any("├─ IXSCAN" in s for s in stages)
        assert any("└─ IXSCAN" in s for s in stages)

    def test_flatten_execution_stats_summary(self):
        explain_result = {
            "queryPlanner": {
                "namespace": "db.coll",
                "winningPlan": {"stage": "COLLSCAN"},
            },
            "executionStats": {
                "executionSuccess": True,
                "nReturned": 5,
                "executionTimeMillis": 3,
                "totalKeysExamined": 0,
                "totalDocsExamined": 42,
            },
        }
        rows = ExplainExecutionPlan.flatten_result(explain_result)
        summary_row = next(r for r in rows if r["stage"] == "executionStats")
        parsed = json.loads(summary_row["details"])
        assert parsed["nReturned"] == 5
        assert parsed["totalDocsExamined"] == 42

    def test_flatten_aggregate_stages(self):
        explain_result = {
            "stages": [
                {
                    "$cursor": {
                        "queryPlanner": {
                            "namespace": "db.coll",
                            "winningPlan": {"stage": "COLLSCAN"},
                        }
                    }
                },
                {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            ],
        }
        rows = ExplainExecutionPlan.flatten_result(explain_result)
        stages = [r["stage"] for r in rows]
        assert any(s.startswith("pipeline[0]: $cursor") for s in stages)
        assert any(s.startswith("pipeline[1]: $group") for s in stages)
        assert any("COLLSCAN" in s for s in stages)

    def test_flatten_fallback_on_empty(self):
        rows = ExplainExecutionPlan.flatten_result({})
        assert len(rows) == 1
        assert rows[0]["stage"] == "explain"


class TestExplainVerbosity:
    """Tests for the optional ``EXPLAIN (key value, ...)`` option clause (grammar-native)."""

    def test_invalid_verbosity_raises(self):
        """An unsupported verbosity identifier fails validation at parse time."""
        from pymongosql.sql.parser import SQLParser

        with pytest.raises(SqlSyntaxError):
            SQLParser("EXPLAIN (verbosity bogus) SELECT * FROM users").get_execution_plan()

    def test_non_select_inner_raises(self):
        """EXPLAIN of a non-SELECT inner statement is rejected at execute time."""
        strategy = ExplainExecution()
        ctx = ExecutionContext("EXPLAIN DELETE FROM users WHERE id = 1")
        with pytest.raises(SqlSyntaxError):
            strategy.execute(ctx, connection=None)

    def test_verbosity_option_is_parsed(self):
        """``EXPLAIN (verbosity executionStats) SELECT ...`` produces the expected plan."""
        from pymongosql.sql.parser import SQLParser

        plan = SQLParser("EXPLAIN (verbosity executionStats) SELECT * FROM users").get_execution_plan()
        assert isinstance(plan, ExplainExecutionPlan)
        assert plan.verbosity == "executionStats"
        assert plan.inner_plan is not None
        assert plan.inner_plan.collection == "users"

    def test_bare_explain_defaults_to_queryPlanner(self):
        from pymongosql.sql.parser import SQLParser

        plan = SQLParser("EXPLAIN SELECT name FROM users WHERE age > 25").get_execution_plan()
        assert isinstance(plan, ExplainExecutionPlan)
        assert plan.verbosity == "queryPlanner"
        assert plan.inner_plan.collection == "users"

    def test_explain_options_are_preserved(self):
        """All option pairs are preserved on the plan even if only 'verbosity' is recognized."""
        from pymongosql.sql.parser import SQLParser

        plan = SQLParser("EXPLAIN (verbosity queryPlanner, format tree) SELECT * FROM users").get_execution_plan()
        assert plan.options == {"verbosity": "queryPlanner", "format": "tree"}


@pytest.mark.integration
class TestExplainIntegration:
    """Integration tests that require a running MongoDB (use the ``conn`` fixture)."""

    def test_explain_simple_select(self, conn):
        cursor = conn.cursor()
        cursor.execute("EXPLAIN SELECT name, email FROM users WHERE age > 25")
        rows = cursor.fetchall()
        assert len(rows) > 0
        col_names = [d[0] for d in cursor.description]
        assert col_names == ["stage", "details"]
        # Winning plan must contribute at least one plan-stage row
        stage_col_idx = col_names.index("stage")
        stage_values = [r[stage_col_idx] for r in rows]
        assert any(
            any(tok in s for tok in ("COLLSCAN", "IXSCAN", "FETCH", "LIMIT", "SORT", "PROJECTION"))
            for s in stage_values
        )

    def test_explain_with_execution_stats(self, conn):
        cursor = conn.cursor()
        cursor.execute("EXPLAIN (verbosity executionStats) SELECT * FROM users LIMIT 5")
        rows = cursor.fetchall()
        col_names = [d[0] for d in cursor.description]
        stage_idx = col_names.index("stage")
        assert any(r[stage_idx] == "executionStats" for r in rows)
