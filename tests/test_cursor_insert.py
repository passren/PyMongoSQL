# -*- coding: utf-8 -*-
"""Test suite for INSERT statement execution via Cursor."""

import pytest

from pymongosql.error import ProgrammingError, SqlSyntaxError
from pymongosql.result_set import ResultSet


class TestCursorInsert:
    """Test suite for INSERT operations using a dedicated test collection."""

    TEST_COLLECTION = "musicians"

    @pytest.fixture(autouse=True)
    def setup_teardown(self, conn):
        """Setup: drop test collection before each test. Teardown: drop after each test."""
        db = conn.database
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)
        yield
        # Teardown: drop the test collection after each test
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)

    def test_insert_single_document(self, conn):
        """Test inserting a single document into the collection."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': 'Alice', 'age': 30, 'city': 'New York'}}"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self

        # Verify the document was inserted
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find())
        assert len(docs) == 1
        assert docs[0]["name"] == "Alice"
        assert docs[0]["age"] == 30
        assert docs[0]["city"] == "New York"

    def test_insert_multiple_documents_via_bag(self, conn):
        """Test inserting multiple documents using bag syntax."""
        sql = (
            f"INSERT INTO {self.TEST_COLLECTION} << "
            "{'name': 'Bob', 'age': 25, 'city': 'Boston'}, "
            "{'name': 'Charlie', 'age': 35, 'city': 'Chicago'} >>"
        )
        cursor = conn.cursor()
        result = cursor.execute("".join(sql))

        assert result == cursor  # execute returns self

        # Verify both documents were inserted
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find({}))
        assert len(docs) == 2

        names = {doc["name"] for doc in docs}
        assert "Bob" in names
        assert "Charlie" in names

    def test_insert_with_null_values(self, conn):
        """Test inserting document with null values."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': 'Diana', 'age': null, 'city': 'Denver'}}"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self

        # Verify document with null was inserted
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find())
        assert len(docs) == 1
        assert docs[0]["name"] == "Diana"
        assert docs[0]["age"] is None
        assert docs[0]["city"] == "Denver"

    def test_insert_with_boolean_and_mixed_types(self, conn):
        """Test inserting document with booleans and various data types."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': 'Eve', 'active': true, 'score': 95.5, 'level': 5}}"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self

        # Verify document with mixed types was inserted
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find())
        assert len(docs) == 1
        assert docs[0]["name"] == "Eve"
        assert docs[0]["active"] is True
        assert docs[0]["score"] == 95.5
        assert docs[0]["level"] == 5

    def test_insert_with_qmark_parameters(self, conn):
        """Test INSERT with qmark (?) placeholder parameters."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': '?', 'age': '?', 'city': '?'}}"
        cursor = conn.cursor()

        # Execute with positional parameters
        result = cursor.execute(sql, ["Frank", 28, "Fresno"])

        assert result == cursor  # execute returns self

        # Verify document was inserted with parameter values
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find())
        assert len(docs) == 1
        assert docs[0]["name"] == "Frank"
        assert docs[0]["age"] == 28
        assert docs[0]["city"] == "Fresno"

    def test_insert_with_named_parameters(self, conn):
        """Test INSERT with qmark (?) placeholder parameters."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': '?', 'age': '?', 'city': '?'}}"
        cursor = conn.cursor()

        # Execute with positional parameters (qmark style)
        result = cursor.execute(sql, ["Grace", 32, "Greensboro"])

        assert result == cursor  # execute returns self

        # Verify document was inserted with parameter values
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find())
        assert len(docs) == 1
        assert docs[0]["name"] == "Grace"
        assert docs[0]["age"] == 32
        assert docs[0]["city"] == "Greensboro"

    def test_insert_multiple_documents_with_parameters(self, conn):
        """Test inserting multiple documents with qmark (?) parameters via bag syntax."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} << {{'name': '?', 'age': '?'}}, {{'name': '?', 'age': '?'}} >>"
        cursor = conn.cursor()

        # Execute with positional parameters for multiple documents
        result = cursor.execute(sql, ["Henry", 40, "Iris", 29])

        assert result == cursor  # execute returns self

        # Verify both documents were inserted with parameter values
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find({}))
        assert len(docs) == 2

        doc_by_name = {doc["name"]: doc for doc in docs}
        assert "Henry" in doc_by_name
        assert doc_by_name["Henry"]["age"] == 40
        assert "Iris" in doc_by_name
        assert doc_by_name["Iris"]["age"] == 29

    def test_insert_with_column_list_and_values(self, conn):
        """Test INSERT with explicit column list and VALUES clause."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} (name, age, city) VALUES ('Kevin', 40, 'Kansas City')"
        cursor = conn.cursor()
        result = cursor.execute(sql)

        assert result == cursor  # execute returns self

        # Verify the document was inserted with correct fields
        db = conn.database
        docs = list(db[self.TEST_COLLECTION].find())
        assert len(docs) == 1
        assert docs[0]["name"] == "Kevin"
        assert docs[0]["age"] == 40
        assert docs[0]["city"] == "Kansas City"

    def test_insert_insufficient_parameters_raises_error(self, conn):
        """Test that insufficient parameters raises ProgrammingError."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': '?', 'age': '?'}}"
        cursor = conn.cursor()

        # Execute with fewer parameters than placeholders
        with pytest.raises(ProgrammingError):
            cursor.execute(sql, ["Jack"])  # Missing second parameter

    def test_insert_missing_named_parameter_raises_error(self, conn):
        """Test that missing named parameter raises ProgrammingError."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': ':name', 'age': ':age'}}"
        cursor = conn.cursor()

        # Execute with incomplete named parameters
        with pytest.raises(ProgrammingError):
            cursor.execute(sql, {"name": "Kate"})  # Missing :age parameter

    def test_insert_invalid_sql_raises_error(self, conn):
        """Test that invalid INSERT SQL raises SqlSyntaxError."""
        sql = f"INSERT INTO {self.TEST_COLLECTION} invalid_syntax"
        cursor = conn.cursor()

        with pytest.raises(SqlSyntaxError):
            cursor.execute(sql)

    def test_insert_followed_by_select(self, conn):
        """Test INSERT followed by SELECT to verify data was persisted."""
        # Insert a document
        insert_sql = f"INSERT INTO {self.TEST_COLLECTION} {{'name': 'Liam', 'score': 88}}"
        cursor = conn.cursor()
        cursor.execute(insert_sql)

        # Select the document back
        select_sql = f"SELECT name, score FROM {self.TEST_COLLECTION} WHERE score > 80"
        result = cursor.execute(select_sql)

        assert result == cursor  # execute returns self
        assert isinstance(cursor.result_set, ResultSet)
        rows = cursor.result_set.fetchall()

        assert len(rows) == 1
        if cursor.result_set.description:
            col_names = [desc[0] for desc in cursor.result_set.description]
            assert "name" in col_names
            assert "score" in col_names
