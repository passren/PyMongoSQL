# -*- coding: utf-8 -*-
import datetime

from pymongosql.sql.parser import SQLParser


class TestWhereClauseFieldOrdering:
    """Test that WHERE clause conditions produce correct filters regardless of field order.

    Regression tests for a bug where boolean values (true/false/null) adjacent to
    AND/OR operators in ANTLR getText() output caused the logical operator to be
    missed, e.g. "B=trueANDA=1" was not split on AND because the word boundary
    check treated "trueAND" as a single word.
    """

    # --- Boolean + Numeric (various orderings) ---

    def test_bool_first_3_conditions(self):
        sql = "SELECT * FROM col WHERE active=true AND age=30 AND score=100"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"active": True}, {"age": 30}, {"score": 100}]}

    def test_bool_middle_3_conditions(self):
        sql = "SELECT * FROM col WHERE age=30 AND active=true AND score=100"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"age": 30}, {"active": True}, {"score": 100}]}

    def test_bool_last_3_conditions(self):
        sql = "SELECT * FROM col WHERE age=30 AND score=100 AND active=true"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"age": 30}, {"score": 100}, {"active": True}]}

    # --- Boolean + String ---

    def test_bool_and_string_bool_first(self):
        sql = "SELECT * FROM col WHERE active=true AND name='John'"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"active": True}, {"name": "John"}]}

    def test_bool_and_string_string_first(self):
        sql = "SELECT * FROM col WHERE name='John' AND active=true"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"name": "John"}, {"active": True}]}

    def test_false_and_string(self):
        sql = "SELECT * FROM col WHERE deleted=false AND status='pending'"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"deleted": False}, {"status": "pending"}]}

    # --- Boolean + String + Numeric (4 conditions) ---

    def test_4_mixed_types(self):
        sql = "SELECT * FROM col WHERE active=true AND name='Alice' AND age=25 AND score>90"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"active": True}, {"name": "Alice"}, {"age": 25}, {"score": {"$gt": 90}}]}

    def test_4_mixed_types_bool_last(self):
        sql = "SELECT * FROM col WHERE name='Alice' AND age=25 AND score>90 AND active=true"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"name": "Alice"}, {"age": 25}, {"score": {"$gt": 90}}, {"active": True}]}

    # --- Datetime with value function ---

    def test_bool_and_datetime_func(self):
        sql = "SELECT * FROM col WHERE active=true AND created_at>str_to_datetime('2024-01-01','%Y-%m-%d')"
        plan = SQLParser(sql).get_execution_plan()
        expected_dt = datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        assert plan.filter_stage == {"$and": [{"active": True}, {"created_at": {"$gt": expected_dt}}]}

    def test_datetime_func_and_bool(self):
        sql = "SELECT * FROM col WHERE created_at>str_to_datetime('2024-01-01','%Y-%m-%d') AND active=true"
        plan = SQLParser(sql).get_execution_plan()
        expected_dt = datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        assert plan.filter_stage == {"$and": [{"created_at": {"$gt": expected_dt}}, {"active": True}]}

    # --- Bracketed / parenthesized groups ---

    def test_brackets_bool_and_num_or_string(self):
        sql = "SELECT * FROM col WHERE (active=true AND age>25) OR status='admin'"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$or": [{"$and": [{"active": True}, {"age": {"$gt": 25}}]}, {"status": "admin"}]}

    def test_brackets_string_or_bool_and_num(self):
        sql = "SELECT * FROM col WHERE status='admin' OR (active=true AND age>25)"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$or": [{"status": "admin"}, {"$and": [{"active": True}, {"age": {"$gt": 25}}]}]}

    def test_brackets_bool_and_string_and_num(self):
        sql = "SELECT * FROM col WHERE (active=true AND name='John') AND age>30"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"$and": [{"active": True}, {"name": "John"}]}, {"age": {"$gt": 30}}]}

    def test_nested_brackets(self):
        sql = "SELECT * FROM col WHERE ((active=true AND age>25) OR status='admin') AND score>50"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {
            "$and": [
                {"$or": [{"$and": [{"active": True}, {"age": {"$gt": 25}}]}, {"status": "admin"}]},
                {"score": {"$gt": 50}},
            ]
        }

    # --- 5+ conditions ---

    def test_5_conditions_all_and(self):
        sql = "SELECT * FROM col WHERE active=true AND name='Bob' AND age>20 AND score<100 AND tier=3"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {
            "$and": [
                {"active": True},
                {"name": "Bob"},
                {"age": {"$gt": 20}},
                {"score": {"$lt": 100}},
                {"tier": 3},
            ]
        }

    def test_5_conditions_bool_scattered(self):
        sql = "SELECT * FROM col WHERE name='Eve' AND active=true AND age=28 AND deleted=false AND score>=50"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {
            "$and": [
                {"name": "Eve"},
                {"active": True},
                {"age": 28},
                {"deleted": False},
                {"score": {"$gte": 50}},
            ]
        }

    # --- OR with multiple booleans ---

    def test_or_with_3_bool_conditions(self):
        sql = "SELECT * FROM col WHERE active=true OR deleted=false OR verified=true"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$or": [{"active": True}, {"deleted": False}, {"verified": True}]}

    # --- Complex mixed AND/OR with brackets ---

    def test_brackets_two_and_groups_with_or(self):
        sql = "SELECT * FROM col WHERE (active=true AND age>25) OR (deleted=false AND status='archived')"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {
            "$or": [
                {"$and": [{"active": True}, {"age": {"$gt": 25}}]},
                {"$and": [{"deleted": False}, {"status": "archived"}]},
            ]
        }

    def test_bool_and_bracketed_or(self):
        sql = "SELECT * FROM col WHERE active=true AND (name='John' OR age>30)"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"active": True}, {"$or": [{"name": "John"}, {"age": {"$gt": 30}}]}]}

    # --- Comparison operators with booleans ---

    def test_bool_not_equal_and_comparison(self):
        sql = "SELECT * FROM col WHERE active!=false AND age>25"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"active": {"$ne": False}}, {"age": {"$gt": 25}}]}

    # --- null mixed with bool ---

    def test_null_bool_and_string(self):
        sql = "SELECT * FROM col WHERE deleted_at=null AND active=true AND name='test'"
        plan = SQLParser(sql).get_execution_plan()
        assert plan.filter_stage == {"$and": [{"deleted_at": None}, {"active": True}, {"name": "test"}]}

    # --- LIKE with bool ---

    def test_like_and_bool(self):
        sql = "SELECT * FROM col WHERE name LIKE '%john%' AND active=true"
        plan = SQLParser(sql).get_execution_plan()
        f = plan.filter_stage
        assert "$and" in f
        assert {"active": True} in f["$and"]
        assert any("name" in cond and "$regex" in cond.get("name", {}) for cond in f["$and"])

    def test_bool_and_like(self):
        sql = "SELECT * FROM col WHERE active=true AND name LIKE '%john%'"
        plan = SQLParser(sql).get_execution_plan()
        f = plan.filter_stage
        assert "$and" in f
        assert {"active": True} in f["$and"]
        assert any("name" in cond and "$regex" in cond.get("name", {}) for cond in f["$and"])

    # --- IN with bool ---

    def test_in_and_bool(self):
        sql = "SELECT * FROM col WHERE status IN ('a','b','c') AND active=true"
        plan = SQLParser(sql).get_execution_plan()
        f = plan.filter_stage
        assert "$and" in f
        assert {"active": True} in f["$and"]
        assert any("status" in cond and "$in" in cond.get("status", {}) for cond in f["$and"])

    def test_bool_and_in(self):
        sql = "SELECT * FROM col WHERE active=true AND status IN ('a','b','c')"
        plan = SQLParser(sql).get_execution_plan()
        f = plan.filter_stage
        assert "$and" in f
        assert {"active": True} in f["$and"]
        assert any("status" in cond and "$in" in cond.get("status", {}) for cond in f["$and"])

    # --- 6 conditions with brackets ---

    def test_6_conditions_with_brackets(self):
        sql = (
            "SELECT * FROM col WHERE (active=true AND deleted=false) "
            "AND (age>20 AND age<60) AND (name='X' OR name='Y')"
        )
        plan = SQLParser(sql).get_execution_plan()
        f = plan.filter_stage
        flat = str(f)
        for key in ["active", "deleted", "age", "name"]:
            assert key in flat, f"Missing expected key '{key}' in filter: {f}"
        assert "$or" in flat
