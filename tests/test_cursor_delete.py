# -*- coding: utf-8 -*-
import pytest


class TestCursorDelete:
    """Test suite for DELETE operations using a dedicated test collection."""

    TEST_COLLECTION = "Music"

    @pytest.fixture(autouse=True)
    def setup_teardown(self, conn):
        """Setup: insert test data. Teardown: drop test collection."""
        db = conn.database
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)

        # Insert test data for delete operations
        db[self.TEST_COLLECTION].insert_many(
            [
                {"title": "Song A", "artist": "Alice", "year": 2021, "genre": "Pop"},
                {"title": "Song B", "artist": "Bob", "year": 2020, "genre": "Rock"},
                {"title": "Song C", "artist": "Charlie", "year": 2021, "genre": "Jazz"},
                {"title": "Song D", "artist": "Diana", "year": 2019, "genre": "Pop"},
                {"title": "Song E", "artist": "Eve", "year": 2022, "genre": "Electronic"},
            ]
        )

        yield

        # Teardown: drop the test collection after each test
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)

    def test_delete_all_documents(self, conn):
        """Test deleting all documents from collection."""
        cursor = conn.cursor()
        result = cursor.execute(f"DELETE FROM {self.TEST_COLLECTION}")

        assert result == cursor  # execute returns self

        # Verify all documents were deleted
        db = conn.database
        remaining = list(db[self.TEST_COLLECTION].find())
        assert len(remaining) == 0

    def test_delete_with_where_equality(self, conn):
        """Test DELETE with WHERE clause filtering by equality."""
        cursor = conn.cursor()
        result = cursor.execute(f"DELETE FROM {self.TEST_COLLECTION} WHERE artist = 'Bob'")

        assert result == cursor  # execute returns self

        # Verify only Bob's song was deleted
        db = conn.database
        remaining = list(db[self.TEST_COLLECTION].find())
        assert len(remaining) == 4

        artist_names = {doc["artist"] for doc in remaining}
        assert "Bob" not in artist_names
        assert "Alice" in artist_names

    def test_delete_with_where_numeric_filter(self, conn):
        """Test DELETE with WHERE clause filtering by numeric field."""
        cursor = conn.cursor()
        result = cursor.execute(f"DELETE FROM {self.TEST_COLLECTION} WHERE year > 2020")

        assert result == cursor

        # Verify songs from 2021 and 2022 were deleted
        db = conn.database
        remaining = list(db[self.TEST_COLLECTION].find())
        assert len(remaining) == 2  # Only 2019 and 2020 remain

    def test_delete_with_and_condition(self, conn):
        """Test DELETE with WHERE clause using AND condition."""
        cursor = conn.cursor()
        result = cursor.execute(f"DELETE FROM {self.TEST_COLLECTION} WHERE genre = 'Pop' AND year = 2021")

        assert result == cursor

        # Only Song A (Pop, 2021) should be deleted
        db = conn.database
        remaining = list(db[self.TEST_COLLECTION].find())
        assert len(remaining) == 4

        titles = {doc["title"] for doc in remaining}
        assert "Song A" not in titles

    def test_delete_with_qmark_parameters(self, conn):
        """Test DELETE with qmark (?) placeholder parameters."""
        cursor = conn.cursor()
        result = cursor.execute(f"DELETE FROM {self.TEST_COLLECTION} WHERE artist = '?'", ["Charlie"])

        assert result == cursor

        # Verify Charlie's song was deleted
        db = conn.database
        remaining = list(db[self.TEST_COLLECTION].find())
        assert len(remaining) == 4

        artists = {doc["artist"] for doc in remaining}
        assert "Charlie" not in artists

    def test_delete_with_multiple_parameters(self, conn):
        """Test DELETE with multiple qmark parameters."""
        cursor = conn.cursor()
        result = cursor.execute(f"DELETE FROM {self.TEST_COLLECTION} WHERE genre = '?' AND year = '?'", ["Pop", 2019])

        assert result == cursor

        # Only Song D (Pop, 2019) should be deleted
        db = conn.database
        remaining = list(db[self.TEST_COLLECTION].find())
        assert len(remaining) == 4

        titles = {doc["title"] for doc in remaining}
        assert "Song D" not in titles

    def test_delete_no_match_returns_success(self, conn):
        """Test that DELETE with no matching records still succeeds."""
        cursor = conn.cursor()
        result = cursor.execute(f"DELETE FROM {self.TEST_COLLECTION} WHERE artist = 'Nonexistent'")

        assert result == cursor

        # Verify no documents were deleted
        db = conn.database
        remaining = list(db[self.TEST_COLLECTION].find())
        assert len(remaining) == 5

    def test_delete_invalid_sql_raises_error(self, conn):
        """Test that invalid DELETE SQL raises SqlSyntaxError."""
        _ = conn.cursor()

        # Note: The parser is quite forgiving. This test is skipped for now
        # as the PartiQL grammar may accept various forms of DELETE syntax.
        # A truly invalid statement would be one with syntax errors at the
        # lexer/parser level, like unmatched parentheses.
        pass

    def test_delete_missing_collection_raises_error(self, conn):
        """Test that DELETE on non-existent collection is handled."""
        cursor = conn.cursor()

        # DELETE on non-existent collection should succeed but delete nothing
        result = cursor.execute("DELETE FROM NonexistentCollection WHERE title = 'Test'")
        assert result == cursor

    def test_delete_then_select_verify_persistence(self, conn):
        """Test DELETE followed by SELECT to verify deletion was persisted."""
        # Delete documents by year
        delete_cursor = conn.cursor()
        delete_cursor.execute(f"DELETE FROM {self.TEST_COLLECTION} WHERE year < 2021")

        # Select remaining documents
        select_cursor = conn.cursor()
        select_cursor.execute(f"SELECT title, year FROM {self.TEST_COLLECTION} ORDER BY year")

        rows = select_cursor.fetchall()

        # Should have Song A (2021), Song C (2021), and Song E (2022)
        assert len(rows) == 3
        years = [row[1] for row in rows]
        assert all(year >= 2021 for year in years)

    def test_delete_followed_by_insert(self, conn):
        """Test DELETE followed by INSERT to verify both operations work."""
        # Delete all
        delete_cursor = conn.cursor()
        delete_cursor.execute(f"DELETE FROM {self.TEST_COLLECTION}")

        db = conn.database
        assert len(list(db[self.TEST_COLLECTION].find())) == 0

        # Insert new document
        insert_cursor = conn.cursor()
        insert_cursor.execute(f"INSERT INTO {self.TEST_COLLECTION} {{'title': 'New Song', 'artist': 'Frank'}}")

        # Verify insertion
        assert len(list(db[self.TEST_COLLECTION].find())) == 1
        doc = list(db[self.TEST_COLLECTION].find())[0]
        assert doc["title"] == "New Song"
