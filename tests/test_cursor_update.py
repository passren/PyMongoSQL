# -*- coding: utf-8 -*-
import pytest


class TestCursorUpdate:
    """Test suite for UPDATE operations using a dedicated test collection."""

    TEST_COLLECTION = "Books"

    @pytest.fixture(autouse=True)
    def setup_teardown(self, conn):
        """Setup: insert test data. Teardown: drop test collection."""
        db = conn.database
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)

        # Insert test data for update operations
        db[self.TEST_COLLECTION].insert_many(
            [
                {"title": "Book A", "author": "Alice", "year": 2020, "price": 29.99, "stock": 10, "available": True},
                {"title": "Book B", "author": "Bob", "year": 2021, "price": 39.99, "stock": 5, "available": True},
                {"title": "Book C", "author": "Charlie", "year": 2019, "price": 19.99, "stock": 0, "available": False},
                {"title": "Book D", "author": "Diana", "year": 2022, "price": 49.99, "stock": 15, "available": True},
                {"title": "Book E", "author": "Eve", "year": 2020, "price": 24.99, "stock": 8, "available": True},
            ]
        )

        yield

        # Teardown: drop the test collection after each test
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)

    def test_update_single_field_all_documents(self, conn):
        """Test updating a single field in all documents."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET available = false")

        assert result == cursor  # execute returns self

        # Verify all documents were updated
        db = conn.database
        updated_docs = list(db[self.TEST_COLLECTION].find())
        assert len(updated_docs) == 5
        assert all(doc["available"] is False for doc in updated_docs)

    def test_update_with_where_equality(self, conn):
        """Test UPDATE with WHERE clause filtering by equality."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET price = 34.99 WHERE author = 'Bob'")

        assert result == cursor

        # Verify only Bob's book was updated
        db = conn.database
        bob_book = db[self.TEST_COLLECTION].find_one({"author": "Bob"})
        assert bob_book is not None
        assert bob_book["price"] == 34.99

        # Verify other books remain unchanged
        alice_book = db[self.TEST_COLLECTION].find_one({"author": "Alice"})
        assert alice_book["price"] == 29.99

    def test_update_multiple_fields(self, conn):
        """Test updating multiple fields in one statement."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET price = 14.99, stock = 20 WHERE title = 'Book C'")

        assert result == cursor

        # Verify multiple fields were updated
        db = conn.database
        book_c = db[self.TEST_COLLECTION].find_one({"title": "Book C"})
        assert book_c is not None
        assert book_c["price"] == 14.99
        assert book_c["stock"] == 20

    def test_update_with_numeric_comparison(self, conn):
        """Test UPDATE with WHERE clause using numeric comparison."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET available = false WHERE stock < 5")

        assert result == cursor

        # Books with stock < 5 should be unavailable (Book C with 0 stock)
        db = conn.database
        unavailable_books = list(db[self.TEST_COLLECTION].find({"available": False}))
        assert len(unavailable_books) >= 1
        assert all(doc["stock"] < 5 for doc in unavailable_books)

    def test_update_with_and_condition(self, conn):
        """Test UPDATE with WHERE clause using AND condition."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET price = 22.99 WHERE year = 2020 AND stock > 5")

        assert result == cursor

        # Only Book E (year=2020, stock=8) should be updated
        db = conn.database
        book_e = db[self.TEST_COLLECTION].find_one({"title": "Book E"})
        assert book_e is not None
        assert book_e["price"] == 22.99

        # Book A (year=2020, stock=10) should also be updated
        book_a = db[self.TEST_COLLECTION].find_one({"title": "Book A"})
        assert book_a is not None
        assert book_a["price"] == 22.99

    def test_update_with_qmark_parameters(self, conn):
        """Test UPDATE with qmark (?) placeholder parameters."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET stock = ? WHERE author = ?", [25, "Alice"])

        assert result == cursor

        # Verify Alice's book stock was updated
        db = conn.database
        alice_book = db[self.TEST_COLLECTION].find_one({"author": "Alice"})
        assert alice_book is not None
        assert alice_book["stock"] == 25

    def test_update_boolean_field(self, conn):
        """Test updating boolean field."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET available = true WHERE stock = 0")

        assert result == cursor

        # Verify Book C (stock=0) is now available
        db = conn.database
        book_c = db[self.TEST_COLLECTION].find_one({"title": "Book C"})
        assert book_c is not None
        assert book_c["available"] is True

    def test_update_with_greater_than(self, conn):
        """Test UPDATE with > operator in WHERE clause."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET price = 59.99 WHERE price > 40")

        assert result == cursor

        # Only Book D (price=49.99) should be updated
        db = conn.database
        book_d = db[self.TEST_COLLECTION].find_one({"title": "Book D"})
        assert book_d is not None
        assert book_d["price"] == 59.99

    def test_update_numeric_to_string(self, conn):
        """Test updating numeric value with string."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET author = 'Anonymous' WHERE year = 2019")

        assert result == cursor

        # Verify Book C author was updated
        db = conn.database
        book_c = db[self.TEST_COLLECTION].find_one({"year": 2019})
        assert book_c is not None
        assert book_c["author"] == "Anonymous"

    def test_update_rowcount(self, conn):
        """Test that rowcount reflects number of updated documents."""
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET available = false WHERE year = 2020")

        # Two books from 2020 (Book A and Book E)
        assert cursor.rowcount == 2

    def test_update_no_matches(self, conn):
        """Test UPDATE with WHERE clause that matches no documents."""
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET price = 99.99 WHERE year = 1999")

        # No documents should be updated
        assert cursor.rowcount == 0

        # Verify all books retain original prices
        db = conn.database
        books = list(db[self.TEST_COLLECTION].find())
        assert all(doc["price"] < 60 for doc in books)

    def test_update_nested_field(self, conn):
        """Test updating nested field using dot notation."""
        # First insert a document with nested structure
        db = conn.database
        db[self.TEST_COLLECTION].insert_one(
            {"title": "Book F", "author": "Frank", "details": {"pages": 300, "publisher": "ABC"}, "year": 2023}
        )

        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET details.pages = 350 WHERE title = 'Book F'")

        assert result == cursor

        # Verify nested field was updated
        book_f = db[self.TEST_COLLECTION].find_one({"title": "Book F"})
        assert book_f is not None
        assert book_f["details"]["pages"] == 350
        assert book_f["details"]["publisher"] == "ABC"  # Other nested field unchanged

    def test_update_set_null(self, conn):
        """Test setting a field to NULL."""
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {self.TEST_COLLECTION} SET stock = null WHERE title = 'Book B'")

        assert result == cursor

        # Verify stock was set to None
        db = conn.database
        book_b = db[self.TEST_COLLECTION].find_one({"title": "Book B"})
        assert book_b is not None
        assert book_b["stock"] is None
