# -*- coding: utf-8 -*-
import pytest
from pymongo.errors import InvalidOperation, OperationFailure

from pymongosql.error import DatabaseError

# Mark tests that require server-side transaction support
transactional = pytest.mark.transactional


class TestConnectionTransaction:
    """Test suite for transaction support in Connection class"""

    def test_autocommit_enabled_by_default(self, conn):
        """Test that autocommit is enabled by default"""
        assert conn.autocommit is True
        assert conn.in_transaction is False
        conn.close()

    def test_in_transaction_initial_state(self, conn):
        """Test in_transaction property is False initially"""
        assert conn.in_transaction is False
        conn.close()

    def test_begin_starts_transaction(self, conn):
        """Test that begin() starts a transaction"""
        conn.begin()

        assert conn.in_transaction is True
        assert conn.autocommit is False
        assert conn.session is not None

        conn.rollback()  # Clean up
        conn.close()

    def test_begin_creates_session(self, conn):
        """Test that begin() creates a session"""
        assert conn.session is None

        conn.begin()

        assert conn.session is not None
        assert conn.session.in_transaction

        conn.rollback()
        conn.close()

    def test_commit_on_empty_transaction(self, conn):
        """Test commit on an empty transaction (no operations)"""
        conn.begin()

        # Commit without any operations should succeed
        conn.commit()

        assert conn.in_transaction is False
        assert conn.autocommit is True

        conn.close()

    def test_rollback_on_empty_transaction(self, conn):
        """Test rollback on an empty transaction (no operations)"""
        conn.begin()

        # Rollback without any operations should succeed
        conn.rollback()

        assert conn.in_transaction is False
        assert conn.autocommit is True

        conn.close()

    def test_commit_without_transaction_is_noop(self, conn):
        """Test that commit() without active transaction is a no-op (DB-API 2.0 compliant)"""
        assert conn.in_transaction is False

        # Calling commit without begin() should not raise
        conn.commit()

        assert conn.in_transaction is False
        assert conn.autocommit is True

        conn.close()

    def test_rollback_without_transaction_is_noop(self, conn):
        """Test that rollback() without active transaction is a no-op (DB-API 2.0 compliant)"""
        assert conn.in_transaction is False

        # Calling rollback without begin() should not raise
        conn.rollback()

        assert conn.in_transaction is False
        assert conn.autocommit is True

        conn.close()

    @transactional
    def test_transaction_with_insert_operation(self, conn):
        """Test transaction with INSERT operation

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Clean up any existing test data
            cursor = conn.cursor()
            cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["transaction_test"])
            conn.commit() if not conn.autocommit else None

            conn.begin()

            try:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["transaction_test", 100])

                assert conn.in_transaction is True

                conn.commit()

                assert conn.in_transaction is False

            finally:
                # Clean up
                cursor = conn.cursor()
                cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["transaction_test"])
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    @transactional
    def test_transaction_with_multiple_operations(self, conn):
        """Test transaction with multiple INSERT operations

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Clean up
            cursor = conn.cursor()
            cursor.execute("DELETE FROM test_transaction WHERE name IN (?, ?)", ["txn_test_1", "txn_test_2"])

            conn.begin()

            try:
                cursor = conn.cursor()

                # First insert
                cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["txn_test_1", 101])

                # Second insert
                cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["txn_test_2", 102])

                assert conn.in_transaction is True

                # Commit both operations atomically
                conn.commit()

                assert conn.in_transaction is False

            finally:
                # Clean up
                cursor = conn.cursor()
                cursor.execute("DELETE FROM test_transaction WHERE name IN (?, ?)", ["txn_test_1", "txn_test_2"])
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    @transactional
    def test_transaction_rollback_undoes_changes(self, conn):
        """Test that rollback() undoes uncommitted changes

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Clean up first
            cursor = conn.cursor()
            cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["rollback_test"])

            conn.begin()

            try:
                cursor = conn.cursor()

                # Insert during transaction
                cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["rollback_test", 200])

                # Verify we're in transaction
                assert conn.in_transaction is True

                # Rollback the transaction
                conn.rollback()

                # Verify transaction is ended
                assert conn.in_transaction is False

                # Verify the insert was rolled back (should not exist)
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM test_transaction WHERE name = ?", ["rollback_test"])
                result = cursor.fetchall()
                assert len(result) == 0, "Insert should have been rolled back"

            finally:
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    @transactional
    def test_transaction_with_update_operation(self, conn):
        """Test transaction with UPDATE operation

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Setup: Insert initial data
            cursor = conn.cursor()
            cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["update_test"])
            cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["update_test", 50])

            conn.begin()

            try:
                cursor = conn.cursor()

                # Update within transaction
                cursor.execute("UPDATE test_transaction SET value = ? WHERE name = ?", [150, "update_test"])

                assert conn.in_transaction is True

                conn.commit()

                assert conn.in_transaction is False

            finally:
                # Clean up
                cursor = conn.cursor()
                cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["update_test"])
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    @transactional
    def test_transaction_with_delete_operation(self, conn):
        """Test transaction with DELETE operation

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Setup: Insert initial data
            cursor = conn.cursor()
            cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["delete_test"])
            cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["delete_test", 300])

            conn.begin()

            try:
                cursor = conn.cursor()

                # Delete within transaction
                cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["delete_test"])

                assert conn.in_transaction is True

                conn.commit()

                assert conn.in_transaction is False

            finally:
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    @transactional
    def test_nested_begin_allowed(self, conn):
        """Test that multiple begin() calls raises error (PyMongo doesn't allow nested transactions)

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            conn.begin()
            session1 = conn.session

            # Begin again - should raise InvalidOperation
            with pytest.raises(InvalidOperation):
                conn.begin()

            # Original session should still be in transaction
            assert conn.in_transaction is True
            assert session1 is not None

            conn.rollback()
            conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    def test_transaction_state_property_setter(self, conn):
        """Test in_transaction property setter"""
        assert conn.in_transaction is False

        # Manually set in_transaction (internal use)
        conn.in_transaction = True
        assert conn.in_transaction is True

        conn.in_transaction = False
        assert conn.in_transaction is False

        conn.close()

    def test_autocommit_disabled_after_begin(self, conn):
        """Test that autocommit is disabled after begin()"""
        assert conn.autocommit is True

        conn.begin()

        assert conn.autocommit is False

        conn.rollback()
        assert conn.autocommit is True

        conn.close()

    @transactional
    def test_transaction_with_query_select(self, conn):
        """Test transaction with SELECT operation (read-only)

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            conn.begin()

            try:
                cursor = conn.cursor()

                # Select should work within transaction
                cursor.execute("SELECT * FROM test_transaction LIMIT ?", [1])
                _ = cursor.fetchone()

                assert conn.in_transaction is True

                conn.commit()

            finally:
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    @transactional
    def test_context_manager_transaction_success(self, conn):
        """Test transaction context manager on successful completion

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Clean up
            cursor = conn.cursor()
            cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["ctx_test"])

            try:
                # Use session context manager
                with conn.session_context():
                    cursor = conn.cursor()
                    cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["ctx_test", 400])

            finally:
                # Clean up
                cursor = conn.cursor()
                cursor.execute("DELETE FROM test_transaction WHERE name = ?", ["ctx_test"])
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    @transactional
    def test_transaction_with_multiple_cursors(self, conn):
        """Test transaction consistency with multiple cursor objects

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Clean up
            cursor1 = conn.cursor()
            cursor1.execute("DELETE FROM test_transaction WHERE name IN (?, ?)", ["cursor_test_1", "cursor_test_2"])

            conn.begin()

            try:
                cursor1 = conn.cursor()
                cursor2 = conn.cursor()

                # Both cursors should see the same transaction
                cursor1.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["cursor_test_1", 501])

                cursor2.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["cursor_test_2", 502])

                # Both operations should be committed together
                conn.commit()

            finally:
                # Clean up
                cursor = conn.cursor()
                cursor.execute("DELETE FROM test_transaction WHERE name IN (?, ?)", ["cursor_test_1", "cursor_test_2"])
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise

    def test_transaction_state_after_rollback(self, conn):
        """Test transaction state is properly reset after rollback"""
        conn.begin()
        assert conn.in_transaction is True
        assert conn.autocommit is False

        conn.rollback()

        assert conn.in_transaction is False
        assert conn.autocommit is True

        conn.close()

    def test_transaction_state_after_commit(self, conn):
        """Test transaction state is properly reset after commit"""
        conn.begin()
        assert conn.in_transaction is True
        assert conn.autocommit is False

        conn.commit()

        assert conn.in_transaction is False
        assert conn.autocommit is True

        conn.close()

    def test_begin_after_commit(self, conn):
        """Test that begin() works after commit()"""
        # First transaction
        conn.begin()
        conn.commit()
        assert conn.in_transaction is False

        # Second transaction
        conn.begin()
        assert conn.in_transaction is True

        conn.rollback()
        conn.close()

    def test_begin_after_rollback(self, conn):
        """Test that begin() works after rollback()"""
        # First transaction
        conn.begin()
        conn.rollback()
        assert conn.in_transaction is False

        # Second transaction
        conn.begin()
        assert conn.in_transaction is True

        conn.rollback()
        conn.close()

    def test_session_created_by_begin(self, conn):
        """Test that session is created/available after begin()"""
        assert conn.session is None

        conn.begin()

        assert conn.session is not None
        assert conn.session.in_transaction

        conn.rollback()
        conn.close()

    @transactional
    def test_multiple_transactions_sequential(self, conn):
        """Test multiple sequential transactions

        NOTE: Requires MongoDB replica set or sharded cluster.
        Will be skipped on standalone servers.
        """
        try:
            # Clean up
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM test_transaction WHERE name IN (?, ?, ?)", ["seq_test_1", "seq_test_2", "seq_test_3"]
            )

            try:
                # First transaction
                conn.begin()
                cursor = conn.cursor()
                cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["seq_test_1", 601])
                conn.commit()

                # Second transaction
                conn.begin()
                cursor = conn.cursor()
                cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["seq_test_2", 602])
                conn.commit()

                # Third transaction
                conn.begin()
                cursor = conn.cursor()
                cursor.execute("INSERT INTO test_transaction {'name': '?', 'value': '?'}", ["seq_test_3", 603])
                conn.commit()

                assert conn.in_transaction is False

            finally:
                # Clean up
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM test_transaction WHERE name IN (?, ?, ?)", ["seq_test_1", "seq_test_2", "seq_test_3"]
                )
                conn.close()
        except (InvalidOperation, OperationFailure, DatabaseError) as e:
            if "Transaction numbers are only allowed on a replica set member or mongos" in str(e):
                pytest.skip("MongoDB server does not support transactions (requires replica set or sharded cluster)")
            raise
