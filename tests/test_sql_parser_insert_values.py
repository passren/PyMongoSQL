# -*- coding: utf-8 -*-
from pymongosql.sql.insert_builder import InsertExecutionPlan
from pymongosql.sql.parser import SQLParser


class TestSQLParserInsertValues:
    """Tests for INSERT parsing with VALUES clause."""

    def test_insert_single_row_implicit_columns(self):
        """Test INSERT with single row without column list."""
        sql = "INSERT INTO users VALUES (1, 'Alice', 30)"
        plan = SQLParser(sql).get_execution_plan()

        assert isinstance(plan, InsertExecutionPlan)
        assert plan.collection == "users"
        assert len(plan.insert_documents) == 1
        assert plan.insert_documents == [{"col0": 1, "col1": "Alice", "col2": 30}]

    def test_insert_multiple_rows_implicit_columns(self):
        """Test INSERT with multiple rows without column list."""
        sql = "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "users"
        assert len(plan.insert_documents) == 2
        assert plan.insert_documents == [
            {"col0": 1, "col1": "Alice", "col2": 30},
            {"col0": 2, "col1": "Bob", "col2": 25},
        ]

    def test_insert_single_row_explicit_columns(self):
        """Test INSERT with single row and explicit column list."""
        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "users"
        assert len(plan.insert_documents) == 1
        assert plan.insert_documents == [{"id": 1, "name": "Alice", "age": 30}]

    def test_insert_multiple_rows_explicit_columns(self):
        """Test INSERT with multiple rows and explicit column list."""
        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "users"
        assert len(plan.insert_documents) == 2
        assert plan.insert_documents == [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}]

    def test_insert_with_null_value(self):
        """Test INSERT with NULL values."""
        sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', NULL)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "users"
        assert plan.insert_documents == [{"id": 1, "name": "Alice", "email": None}]

    def test_insert_with_boolean_values(self):
        """Test INSERT with boolean values."""
        sql = "INSERT INTO flags (id, is_active, is_deleted) VALUES (1, TRUE, FALSE)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "flags"
        assert plan.insert_documents == [{"id": 1, "is_active": True, "is_deleted": False}]

    def test_insert_with_positional_parameters(self):
        """Test INSERT with positional parameters (?)."""
        sql = "INSERT INTO users (id, name) VALUES (?, ?)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "users"
        assert plan.insert_documents == [{"id": "?", "name": "?"}]

    def test_insert_with_numeric_values(self):
        """Test INSERT with integer and decimal values."""
        sql = "INSERT INTO products (id, name, price, quantity) VALUES (1, 'Widget', 19.99, 100)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "products"
        assert plan.insert_documents == [{"id": 1, "name": "Widget", "price": 19.99, "quantity": 100}]

    def test_insert_empty_string(self):
        """Test INSERT with empty string."""
        sql = "INSERT INTO users (id, name) VALUES (1, '')"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "users"
        assert plan.insert_documents == [{"id": 1, "name": ""}]

    def test_insert_multiple_rows_mixed_types(self):
        """Test INSERT with multiple rows containing mixed types."""
        sql = "INSERT INTO data (id, data_value) VALUES (1, 'text'), (2, 42), (3, NULL)"
        plan = SQLParser(sql).get_execution_plan()

        assert plan.collection == "data"
        assert len(plan.insert_documents) == 3
        assert plan.insert_documents == [
            {"id": 1, "data_value": "text"},
            {"id": 2, "data_value": 42},
            {"id": 3, "data_value": None},
        ]

    def test_insert_values_single_row(self):
        sql = (
            "INSERT INTO Films (code, title, did, date_prod, kind) "
            "VALUES ('B6717', 'Tampopo', 110, '1985-02-10', 'Comedy')"
        )
        plan = SQLParser("".join(sql)).get_execution_plan()

        assert isinstance(plan, InsertExecutionPlan)
        assert plan.collection == "Films"
        assert plan.insert_documents == [
            {
                "code": "B6717",
                "title": "Tampopo",
                "did": 110,
                "date_prod": "1985-02-10",
                "kind": "Comedy",
            }
        ]

    def test_insert_values_multiple_rows(self):
        sql = (
            "INSERT INTO Films (code, title, did, date_prod, kind) "
            "VALUES ('B6717', 'Tampopo', 110, '1985-02-10', 'Comedy'),"
            "       ('HG120', 'The Dinner Game', 140, NULL, 'Comedy')"
        )
        plan = SQLParser("".join(sql)).get_execution_plan()

        assert isinstance(plan, InsertExecutionPlan)
        assert plan.collection == "Films"
        assert plan.insert_documents == [
            {
                "code": "B6717",
                "title": "Tampopo",
                "did": 110,
                "date_prod": "1985-02-10",
                "kind": "Comedy",
            },
            {
                "code": "HG120",
                "title": "The Dinner Game",
                "did": 140,
                "date_prod": None,
                "kind": "Comedy",
            },
        ]
