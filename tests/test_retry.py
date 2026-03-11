# -*- coding: utf-8 -*-
import pytest
from pymongo.errors import ConnectionFailure, NetworkTimeout

from pymongosql.connection import Connection
from pymongosql.error import OperationalError
from pymongosql.result_set import ResultSet
from pymongosql.retry import RetryConfig, execute_with_retry
from pymongosql.sql.query_builder import QueryExecutionPlan


def test_execute_with_retry_succeeds_after_transient_failures():
    state = {"calls": 0}

    def flaky_operation():
        state["calls"] += 1
        if state["calls"] < 3:
            raise NetworkTimeout("temporary timeout")
        return "ok"

    result = execute_with_retry(
        flaky_operation,
        RetryConfig(enabled=True, attempts=3, wait_min=0.0, wait_max=0.0),
        "unit flaky operation",
    )

    assert result == "ok"
    assert state["calls"] == 3


def test_execute_with_retry_disabled_does_not_retry():
    state = {"calls": 0}

    def flaky_operation():
        state["calls"] += 1
        raise NetworkTimeout("temporary timeout")

    with pytest.raises(NetworkTimeout):
        execute_with_retry(
            flaky_operation,
            RetryConfig(enabled=False, attempts=5, wait_min=0.0, wait_max=0.0),
            "unit no-retry operation",
        )

    assert state["calls"] == 1


def test_execute_with_retry_non_retryable_exception_is_not_retried():
    state = {"calls": 0}

    def non_retryable_operation():
        state["calls"] += 1
        raise ValueError("not retryable")

    with pytest.raises(ValueError):
        execute_with_retry(
            non_retryable_operation,
            RetryConfig(enabled=True, attempts=5, wait_min=0.0, wait_max=0.0),
            "unit non-retryable operation",
        )

    assert state["calls"] == 1


def test_connection_ping_retries_and_connects(monkeypatch):
    state = {"calls": 0}

    class FakeAdmin:
        def command(self, command_name):
            state["calls"] += 1
            if state["calls"] < 3:
                raise ConnectionFailure("transient connection issue")
            assert command_name == "ping"
            return {"ok": 1}

    class FakeClient:
        def __init__(self, **kwargs):
            self.admin = FakeAdmin()
            self.nodes = {("localhost", 27017)}

        def get_database(self, database_name):
            return object()

        def close(self):
            return None

    monkeypatch.setattr("pymongosql.connection.MongoClient", lambda **kwargs: FakeClient(**kwargs))

    conn = Connection(
        host="mongodb://localhost:27017/test_db",
        database="test_db",
        retry_enabled=True,
        retry_attempts=3,
        retry_wait_min=0.0,
        retry_wait_max=0.0,
    )
    try:
        assert conn.is_connected
        assert state["calls"] == 3
    finally:
        conn.close()


def test_connection_ping_retry_exhausted_raises_operational_error(monkeypatch):
    state = {"calls": 0}

    class FakeAdmin:
        def command(self, command_name):
            state["calls"] += 1
            raise ConnectionFailure("always failing")

    class FakeClient:
        def __init__(self, **kwargs):
            self.admin = FakeAdmin()
            self.nodes = {("localhost", 27017)}

        def get_database(self, database_name):
            return object()

        def close(self):
            return None

    monkeypatch.setattr("pymongosql.connection.MongoClient", lambda **kwargs: FakeClient(**kwargs))

    with pytest.raises(OperationalError):
        Connection(
            host="mongodb://localhost:27017/test_db",
            database="test_db",
            retry_enabled=True,
            retry_attempts=2,
            retry_wait_min=0.0,
            retry_wait_max=0.0,
        )

    assert state["calls"] == 2


def test_result_set_getmore_retries_transient_failures():
    state = {"calls": 0}

    class FakeDatabase:
        def command(self, command_payload):
            state["calls"] += 1
            if state["calls"] < 3:
                raise NetworkTimeout("temporary getMore timeout")
            assert command_payload.get("getMore") == 999
            return {
                "cursor": {
                    "id": 0,
                    "nextBatch": [{"name": "retry-user"}],
                }
            }

    result_set = ResultSet(
        command_result={"cursor": {"id": 999, "firstBatch": []}},
        execution_plan=QueryExecutionPlan(collection="users", projection_stage={"name": 1}),
        database=FakeDatabase(),
        retry_config=RetryConfig(enabled=True, attempts=3, wait_min=0.0, wait_max=0.0),
    )

    row = result_set.fetchone()
    assert row is not None
    assert row[0] == "retry-user"
    assert state["calls"] == 3
