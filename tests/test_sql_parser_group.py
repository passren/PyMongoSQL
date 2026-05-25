# -*- coding: utf-8 -*-
import json

from pymongosql.sql.parser import SQLParser


class TestCountStarParsing:
    """Test that COUNT(*) in SQL is translated to a MongoDB aggregate pipeline."""

    def test_count_star_basic(self):
        """SELECT COUNT(*) FROM users → aggregate with $group and $sum:1"""
        sql = "SELECT COUNT(*) FROM users"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        assert plan.collection == "users"

        pipeline = json.loads(plan.aggregate_pipeline)
        # Should have $group and $project stages
        assert len(pipeline) == 2
        assert "$group" in pipeline[0]
        assert pipeline[0]["$group"]["_id"] is None
        assert pipeline[0]["$group"]["COUNT(*)"] == {"$sum": 1}

    def test_count_star_with_alias(self):
        """SELECT COUNT(*) AS total FROM users → alias used in $group"""
        sql = "SELECT COUNT(*) AS total FROM users"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        assert pipeline[0]["$group"]["total"] == {"$sum": 1}
        # $project should expose the alias
        assert pipeline[1]["$project"]["total"] == 1
        assert pipeline[1]["$project"]["_id"] == 0

    def test_count_star_with_alias_no_as(self):
        """SELECT COUNT(*) total FROM users → alias without AS keyword"""
        sql = "SELECT COUNT(*) total FROM users"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        assert pipeline[0]["$group"]["total"] == {"$sum": 1}

    def test_count_star_with_where(self):
        """SELECT COUNT(*) AS total FROM users WHERE age > 25 → $match before $group"""
        sql = "SELECT COUNT(*) AS total FROM users WHERE age > 25"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        # Should have $match, $group, $project
        assert len(pipeline) == 3
        assert "$match" in pipeline[0]
        assert "$group" in pipeline[1]
        assert pipeline[1]["$group"]["total"] == {"$sum": 1}

    def test_count_star_projection_stage(self):
        """Projection stage should reflect aggregate output fields."""
        sql = "SELECT COUNT(*) AS total FROM users"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.projection_stage == {"total": 1}

    def test_count_star_plan_validates(self):
        """Generated aggregate plan should pass validation."""
        sql = "SELECT COUNT(*) FROM users"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()
        assert plan.validate() is True

    def test_sum(self):
        """SELECT SUM(price) AS total_price FROM products"""
        sql = "SELECT SUM(price) AS total_price FROM products"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        assert pipeline[0]["$group"]["total_price"] == {"$sum": "$price"}

    def test_avg(self):
        """SELECT AVG(age) AS avg_age FROM users"""
        sql = "SELECT AVG(age) AS avg_age FROM users"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        assert pipeline[0]["$group"]["avg_age"] == {"$avg": "$age"}

    def test_min(self):
        """SELECT MIN(price) AS cheapest FROM products"""
        sql = "SELECT MIN(price) AS cheapest FROM products"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        assert pipeline[0]["$group"]["cheapest"] == {"$min": "$price"}

    def test_max(self):
        """SELECT MAX(price) AS most_expensive FROM products"""
        sql = "SELECT MAX(price) AS most_expensive FROM products"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        assert pipeline[0]["$group"]["most_expensive"] == {"$max": "$price"}

    def test_multiple_aggregates(self):
        """SELECT COUNT(*) AS cnt, AVG(price) AS avg_price, MAX(price) AS max_price FROM products"""
        sql = "SELECT COUNT(*) AS cnt, AVG(price) AS avg_price, MAX(price) AS max_price FROM products"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        group = pipeline[0]["$group"]
        assert group["_id"] is None
        assert group["cnt"] == {"$sum": 1}
        assert group["avg_price"] == {"$avg": "$price"}
        assert group["max_price"] == {"$max": "$price"}
        # $project exposes all three
        project = pipeline[1]["$project"]
        assert project == {"_id": 0, "cnt": 1, "avg_price": 1, "max_price": 1}

    def test_aggregate_with_nested_field(self):
        """SELECT SUM(details.total) AS revenue FROM orders"""
        sql = "SELECT SUM(details.total) AS revenue FROM orders"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is True
        pipeline = json.loads(plan.aggregate_pipeline)
        assert pipeline[0]["$group"]["revenue"] == {"$sum": "$details.total"}

    def test_aggregate_no_alias_uses_raw_text(self):
        """SELECT SUM(price) FROM products → alias defaults to SUM(price)"""
        sql = "SELECT SUM(price) FROM products"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        pipeline = json.loads(plan.aggregate_pipeline)
        assert "SUM(price)" in pipeline[0]["$group"]

    def test_regular_select_unaffected(self):
        """Regular SELECT without aggregate functions should not be affected."""
        sql = "SELECT name, email FROM users"
        parser = SQLParser(sql)
        plan = parser.get_execution_plan()

        assert plan.is_aggregate_query is False
        assert plan.projection_stage == {"name": 1, "email": 1}
