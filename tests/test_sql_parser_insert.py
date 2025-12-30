# -*- coding: utf-8 -*-
import pytest

from pymongosql.error import SqlSyntaxError
from pymongosql.sql.insert_builder import InsertExecutionPlan
from pymongosql.sql.parser import SQLParser


class TestSQLParserInsert:
    """Tests for INSERT parsing via AST visitor (PartiQL-style)."""

    def test_insert_single_object_literal(self):
        sql = "INSERT INTO users {'id': 1, 'name': 'Jane', 'age': 30}"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, InsertExecutionPlan)
        assert plan.collection == "users"
        assert plan.insert_documents == [{"id": 1, "name": "Jane", "age": 30}]

    def test_insert_bag_documents(self):
        sql = "INSERT INTO items << {'a': 1}, {'a': 2, 'b': 'x'} >>"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "items"
        assert plan.insert_documents == [{"a": 1}, {"a": 2, "b": "x"}]

    def test_insert_literals_lowercase(self):
        sql = "INSERT INTO flags {'is_on': null, 'is_new': true, 'note': 'ok'}"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "flags"
        assert plan.insert_documents == [{"is_on": None, "is_new": True, "note": "ok"}]

    def test_insert_object_qmark_parameters(self):
        sql = "INSERT INTO orders {'id': '?', 'total': '?'}"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "orders"
        assert plan.insert_documents == [{"id": "?", "total": "?"}]
        assert plan.parameter_style == "qmark"
        assert plan.parameter_count == 2

    def test_insert_object_named_parameters(self):
        sql = "INSERT INTO orders {'id': ':id', 'total': ':total'}"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "orders"
        assert plan.insert_documents == [{"id": ":id", "total": ":total"}]
        assert plan.parameter_style == "named"
        assert plan.parameter_count == 2

    def test_insert_bag_named_parameters(self):
        sql = "INSERT INTO items << {'a': ':one'}, {'a': ':two'} >>"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "items"
        assert plan.insert_documents == [{"a": ":one"}, {"a": ":two"}]
        assert plan.parameter_style == "named"
        assert plan.parameter_count == 2

    def test_insert_mixed_parameter_styles_fails(self):
        sql = "INSERT INTO items << {'a': '?'}, {'a': ':b'} >>"
        with pytest.raises(SqlSyntaxError):
            SQLParser(sql).get_execution_plan()

    def test_insert_single_tuple(self):
        sql = "INSERT INTO Films {'code': 'B6717', 'did': 110, 'date_prod': '1985-02-10', 'kind': 'Comedy'}"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, InsertExecutionPlan)
        assert plan.collection == "Films"
        assert plan.insert_documents == [
            {
                "code": "B6717",
                "did": 110,
                "date_prod": "1985-02-10",
                "kind": "Comedy",
            }
        ]

    def test_insert_invalid_expression_raises(self):
        sql = "INSERT INTO users 123"
        with pytest.raises(SqlSyntaxError):
            SQLParser(sql).get_execution_plan()
