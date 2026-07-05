# -*- coding: utf-8 -*-
import pytest

from tests.conftest import HAS_PANDAS, HAS_SQLALCHEMY

if HAS_PANDAS:
    import pandas as pd


pytestmark = pytest.mark.skipif(
    not (HAS_SQLALCHEMY and HAS_PANDAS),
    reason="Pandas and SQLAlchemy are required for pandas SQLAlchemy integration tests",
)


class TestPandasSQLAlchemy:
    """Compatibility tests for pandas through the SQLAlchemy engine."""

    TEST_COLLECTION = "test_pandas_sqlalchemy"

    @pytest.fixture(autouse=True)
    def setup_teardown(self, conn):
        """Keep pandas write tests isolated from shared seeded collections."""
        db = conn.database
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)
        yield
        if self.TEST_COLLECTION in db.list_collection_names():
            db.drop_collection(self.TEST_COLLECTION)

    def test_read_sql_returns_dataframe(self, sqlalchemy_engine):
        """pandas.read_sql should load a projected query into a DataFrame."""
        query = "SELECT _id, name, age FROM users LIMIT 5"

        dataframe = pd.read_sql(query, sqlalchemy_engine)

        assert isinstance(dataframe, pd.DataFrame)
        assert not dataframe.empty
        assert list(dataframe.columns) == ["_id", "name", "age"]
        assert len(dataframe) <= 5
        assert dataframe["_id"].notna().all()
        assert dataframe["name"].notna().all()

    def test_to_sql_append_writes_rows(self, sqlalchemy_engine, conn):
        """pandas should support read_sql, iloc selection, and to_sql append."""
        db = conn.database
        source = pd.read_sql(
            "SELECT _id, name, age, city, active FROM users LIMIT 5",
            sqlalchemy_engine,
        )
        selected = source.iloc[[1, 3]].copy()
        selected["source_collection"] = "users"

        selected.to_sql(self.TEST_COLLECTION, sqlalchemy_engine, if_exists="append", index=False)

        docs = list(
            db[self.TEST_COLLECTION]
            .find({}, {"_id": 1, "name": 1, "age": 1, "active": 1, "source_collection": 1})
            .sort("name", 1)
        )

        expected = (
            selected[["_id", "name", "age", "active", "source_collection"]].sort_values("name").to_dict("records")
        )

        assert len(docs) == len(expected)
        assert docs == expected

        round_trip = pd.read_sql(
            f"SELECT _id, name, age, active, source_collection FROM {self.TEST_COLLECTION}",
            sqlalchemy_engine,
        )
        round_trip = round_trip.sort_values("name").reset_index(drop=True)
        expected_frame = (
            selected[["_id", "name", "age", "active", "source_collection"]].sort_values("name").reset_index(drop=True)
        )

        assert isinstance(round_trip, pd.DataFrame)
        assert list(round_trip.columns) == ["_id", "name", "age", "active", "source_collection"]
        assert round_trip.to_dict("records") == expected_frame.to_dict("records")

    def test_to_sql_replace_recreates_collection(self, sqlalchemy_engine, conn):
        """pandas.to_sql with replace should drop and recreate the collection transparently."""
        db = conn.database
        db[self.TEST_COLLECTION].insert_one({"_id": "stale", "name": "stale"})

        replacement = pd.read_sql(
            "SELECT _id, name, age, active FROM users LIMIT 2",
            sqlalchemy_engine,
        ).copy()
        replacement["source_collection"] = "users"

        replacement.to_sql(self.TEST_COLLECTION, sqlalchemy_engine, if_exists="replace", index=False)

        docs = list(
            db[self.TEST_COLLECTION]
            .find({}, {"_id": 1, "name": 1, "age": 1, "active": 1, "source_collection": 1})
            .sort("_id", 1)
        )
        expected = replacement.sort_values("_id").reset_index(drop=True)

        assert len(docs) == len(expected)
        assert docs == expected.to_dict("records")
