# -*- coding: utf-8 -*-
from pymongosql.sql.parser import SQLParser


def test_qualified_aggregate_call_parsing():
    """Test parsing of collection.aggregate('pipeline', 'options') syntax"""
    sql = """
    SELECT name, email
    FROM users.aggregate('[{"$match": {"active": true}}]', '{}')
    """

    parser = SQLParser(sql)
    execution_plan = parser.get_execution_plan()

    # Should detect it's an aggregate operation
    assert execution_plan.collection == "users"
    assert execution_plan.aggregate_pipeline is not None
    assert execution_plan.aggregate_pipeline == '[{"$match": {"active": true}}]'
    assert execution_plan.aggregate_options == "{}"


def test_unqualified_aggregate_call_parsing():
    """Test parsing of aggregate('pipeline', 'options') syntax (collection agnostic)"""
    sql = """
    SELECT *
    FROM aggregate('[{"$group": {"_id": "$category", "total": {"$sum": "$price"}}}]', '{}')
    """

    parser = SQLParser(sql)
    execution_plan = parser.get_execution_plan()

    # Should detect it's an aggregate operation without explicit collection
    assert execution_plan.collection is None
    assert execution_plan.aggregate_pipeline is not None
    assert execution_plan.aggregate_pipeline == '[{"$group": {"_id": "$category", "total": {"$sum": "$price"}}}]'


def test_aggregate_with_projection():
    """Test that projections still work with aggregate calls"""
    sql = """
    SELECT a, b
    FROM collection.aggregate('[{"$match": {"status": "active"}}]', '{}')
    """

    parser = SQLParser(sql)
    execution_plan = parser.get_execution_plan()

    # Should still have projection for SELECT a, b
    assert execution_plan.projection_stage is not None
    assert "a" in execution_plan.projection_stage
    assert "b" in execution_plan.projection_stage


def test_aggregate_with_where_clause():
    """Test aggregate call with additional WHERE clause"""
    sql = """
    SELECT *
    FROM orders.aggregate('[{"$match": {"total": {"$gt": 100}}}]', '{}')
    WHERE status = 'completed'
    """

    parser = SQLParser(sql)
    execution_plan = parser.get_execution_plan()

    # Should combine both aggregate pipeline and WHERE conditions
    assert execution_plan.aggregate_pipeline is not None
    # The WHERE clause should create additional filter stage
    assert execution_plan.filter_stage is not None


def test_aggregate_with_sort_and_limit():
    """Test aggregate call with ORDER BY and LIMIT"""
    sql = """
    SELECT *
    FROM sales.aggregate('[{"$group": {"_id": "$product", "total": {"$sum": "$amount"}}}]', '{}')
    ORDER BY total DESC
    LIMIT 10
    """

    parser = SQLParser(sql)
    execution_plan = parser.get_execution_plan()

    assert execution_plan.aggregate_pipeline is not None
    assert execution_plan.sort_stage is not None
    assert execution_plan.limit_stage == 10
