#!/usr/bin/env python3
import pytest

from tests.conftest import HAS_SQLALCHEMY, Base

try:
    from sqlalchemy import Boolean, Column, Float, Integer, String, text
except ImportError:
    pass

# Skip all tests if SQLAlchemy is not available
pytestmark = pytest.mark.skipif(not HAS_SQLALCHEMY, reason="SQLAlchemy not available")


# ORM Models for testing
if HAS_SQLALCHEMY:

    class User(Base):
        """User model for DML operations testing."""

        __tablename__ = "test_users_orm"

        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        email = Column(String(100))
        age = Column(Integer)
        is_active = Column(Boolean, default=True)

    class Product(Base):
        """Product model for DML operations testing."""

        __tablename__ = "test_products_orm"

        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        price = Column(Float)
        quantity = Column(Integer)
        in_stock = Column(Boolean, default=True)


class TestSQLAlchemyDML:
    """Test SQLAlchemy dialect with DML operations (INSERT, UPDATE, DELETE)."""

    def test_insert_single_row_explicit_columns(self, sqlalchemy_engine, conn):
        """Test INSERT with single row and explicit columns via SQLAlchemy."""
        with sqlalchemy_engine.begin() as connection:
            # Clean up test data first
            try:
                connection.execute(text("DELETE FROM test_insert_values"))
            except Exception:
                pass

            # Insert single row with VALUES clause
            sql = "INSERT INTO test_insert_values (id, name, age) VALUES (1, 'Alice', 30)"
            connection.execute(text(sql))

            # Verify the insert
            result = connection.execute(text("SELECT id, name, age FROM test_insert_values WHERE id = 1"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("id") == 1
                assert row._mapping.get("name") == "Alice"
                assert row._mapping.get("age") == 30
            else:
                assert row[0] == 1
                assert row[1] == "Alice"
                assert row[2] == 30

            # Clean up
            connection.execute(text("DELETE FROM test_insert_values"))

    def test_insert_multiple_rows_explicit_columns(self, sqlalchemy_engine, conn):
        """Test INSERT with multiple rows and explicit columns via SQLAlchemy."""
        with sqlalchemy_engine.begin() as connection:
            # Clean up test data first
            try:
                connection.execute(text("DELETE FROM test_insert_values"))
            except Exception:
                pass

            # Insert multiple rows with VALUES clause
            sql = "INSERT INTO test_insert_values (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)"
            connection.execute(text(sql))

            # Verify the inserts
            result = connection.execute(text("SELECT id, name, age FROM test_insert_values ORDER BY id"))
            rows = result.fetchall()

            assert len(rows) == 2

            # Check first row
            if hasattr(rows[0], "_mapping"):
                assert rows[0]._mapping.get("id") == 1
                assert rows[0]._mapping.get("name") == "Alice"
                assert rows[0]._mapping.get("age") == 30
                assert rows[1]._mapping.get("id") == 2
                assert rows[1]._mapping.get("name") == "Bob"
                assert rows[1]._mapping.get("age") == 25
            else:
                assert rows[0][0] == 1
                assert rows[0][1] == "Alice"
                assert rows[0][2] == 30
                assert rows[1][0] == 2
                assert rows[1][1] == "Bob"
                assert rows[1][2] == 25

            # Clean up
            connection.execute(text("DELETE FROM test_insert_values"))

    def test_insert_single_row_implicit_columns(self, sqlalchemy_engine, conn):
        """Test INSERT with single row without column list via SQLAlchemy."""
        with sqlalchemy_engine.begin() as connection:
            # Clean up test data first
            try:
                connection.execute(text("DELETE FROM test_insert_implicit"))
            except Exception:
                pass

            # Insert single row with VALUES clause (implicit columns)
            sql = "INSERT INTO test_insert_implicit VALUES (1, 'Alice', 30)"
            connection.execute(text(sql))

            # Verify the insert (auto-named columns: col0, col1, col2)
            result = connection.execute(text("SELECT col0, col1, col2 FROM test_insert_implicit WHERE col0 = 1"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("col0") == 1
                assert row._mapping.get("col1") == "Alice"
                assert row._mapping.get("col2") == 30
            else:
                assert row[0] == 1
                assert row[1] == "Alice"
                assert row[2] == 30

            # Clean up
            connection.execute(text("DELETE FROM test_insert_implicit"))

    def test_insert_with_null_values(self, sqlalchemy_engine, conn):
        """Test INSERT with NULL values via SQLAlchemy."""
        with sqlalchemy_engine.begin() as connection:
            # Clean up test data first
            try:
                connection.execute(text("DELETE FROM test_insert_null"))
            except Exception:
                pass

            # Insert with NULL value
            sql = "INSERT INTO test_insert_null (id, name, email) VALUES (1, 'Alice', NULL)"
            connection.execute(text(sql))

            # Verify the insert
            result = connection.execute(text("SELECT id, name, email FROM test_insert_null WHERE id = 1"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("id") == 1
                assert row._mapping.get("name") == "Alice"
                assert row._mapping.get("email") is None
            else:
                assert row[0] == 1
                assert row[1] == "Alice"
                assert row[2] is None

            # Clean up
            connection.execute(text("DELETE FROM test_insert_null"))

    def test_insert_with_boolean_values(self, sqlalchemy_engine, conn):
        """Test INSERT with boolean values via SQLAlchemy."""
        with sqlalchemy_engine.begin() as connection:
            # Clean up test data first
            try:
                connection.execute(text("DELETE FROM test_insert_bool"))
            except Exception:
                pass

            # Insert with boolean values
            sql = "INSERT INTO test_insert_bool (id, is_active, is_deleted) VALUES (1, TRUE, FALSE)"
            connection.execute(text(sql))

            # Verify the insert
            result = connection.execute(text("SELECT id, is_active, is_deleted FROM test_insert_bool WHERE id = 1"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("id") == 1
                assert row._mapping.get("is_active") is True
                assert row._mapping.get("is_deleted") is False
            else:
                assert row[0] == 1
                assert row[1] is True
                assert row[2] is False

            # Clean up
            connection.execute(text("DELETE FROM test_insert_bool"))

    def test_orm_table_insert_single_row(self, session_maker, conn):
        """Test INSERT VALUES with ORM table using raw SQL."""
        session = session_maker()
        try:
            # Clean up first
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()

            # Insert using VALUES clause with ORM table
            sql = """
                INSERT INTO test_users_orm (id, name, email, age, is_active)
                VALUES (1, 'Alice', 'alice@example.com', 30, TRUE)
            """
            session.execute(text(sql))
            session.commit()

            # Verify the insert
            result = session.execute(text("SELECT id, name, email, age, is_active FROM test_users_orm WHERE id = 1"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("id") == 1
                assert row._mapping.get("name") == "Alice"
                assert row._mapping.get("email") == "alice@example.com"
                assert row._mapping.get("age") == 30
                assert row._mapping.get("is_active") is True
            else:
                assert row[0] == 1
                assert row[1] == "Alice"
                assert row[2] == "alice@example.com"
                assert row[3] == 30
                assert row[4] is True

        finally:
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()
            session.close()

    def test_orm_table_insert_multiple_rows(self, session_maker, conn):
        """Test INSERT VALUES with ORM table using raw SQL for multiple rows."""
        session = session_maker()
        try:
            # Clean up first
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()

            # Insert multiple rows using VALUES clause
            sql = """
                INSERT INTO test_users_orm (id, name, email, age, is_active)
                VALUES
                    (1, 'Alice', 'alice@example.com', 30, TRUE),
                    (2, 'Bob', 'bob@example.com', 25, TRUE),
                    (3, 'Charlie', 'charlie@example.com', 35, FALSE)
            """
            session.execute(text(sql))
            session.commit()

            # Verify the inserts
            result = session.execute(text("SELECT id, name, age, is_active FROM test_users_orm ORDER BY id"))
            rows = result.fetchall()

            assert len(rows) == 3
            if hasattr(rows[0], "_mapping"):
                assert rows[0]._mapping.get("id") == 1
                assert rows[0]._mapping.get("name") == "Alice"
                assert rows[0]._mapping.get("is_active") is True
                assert rows[1]._mapping.get("id") == 2
                assert rows[1]._mapping.get("name") == "Bob"
                assert rows[2]._mapping.get("id") == 3
                assert rows[2]._mapping.get("name") == "Charlie"
                assert rows[2]._mapping.get("is_active") is False
            else:
                assert rows[0][0] == 1
                assert rows[0][1] == "Alice"
                assert rows[1][0] == 2
                assert rows[1][1] == "Bob"
                assert rows[2][0] == 3
                assert rows[2][1] == "Charlie"

        finally:
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()
            session.close()

    def test_orm_table_update_single_row(self, session_maker, conn):
        """Test UPDATE using ORM table with raw SQL."""
        session = session_maker()
        try:
            # Clean up and insert test data
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()

            session.execute(
                text(
                    "INSERT INTO test_users_orm (id, name, email, age, is_active) "
                    "VALUES (1, 'Alice', 'alice@example.com', 30, TRUE)"
                )
            )
            session.commit()

            # Update the user
            session.execute(
                text("UPDATE test_users_orm SET age = 31, email = 'alice.updated@example.com' WHERE id = 1")
            )
            session.commit()

            # Verify the update
            result = session.execute(text("SELECT id, name, email, age FROM test_users_orm WHERE id = 1"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("id") == 1
                assert row._mapping.get("name") == "Alice"
                assert row._mapping.get("email") == "alice.updated@example.com"
                assert row._mapping.get("age") == 31
            else:
                assert row[0] == 1
                assert row[1] == "Alice"
                assert row[2] == "alice.updated@example.com"
                assert row[3] == 31

        finally:
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()
            session.close()

    def test_orm_table_update_multiple_rows(self, session_maker, conn):
        """Test UPDATE using ORM table with multiple rows."""
        session = session_maker()
        try:
            # Clean up and insert test data
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()

            session.execute(
                text(
                    "INSERT INTO test_products_orm (id, name, price, quantity, in_stock) VALUES "
                    "(1, 'Widget', 19.99, 100, TRUE), "
                    "(2, 'Gadget', 29.99, 50, TRUE), "
                    "(3, 'Tool', 9.99, 0, FALSE)"
                )
            )
            session.commit()

            # Update multiple products - set quantity to 200 for in-stock items
            session.execute(text("UPDATE test_products_orm SET quantity = 200 WHERE in_stock = TRUE"))
            session.commit()

            # Verify the updates
            result = session.execute(text("SELECT id, name, quantity, in_stock FROM test_products_orm ORDER BY id"))
            rows = result.fetchall()

            assert len(rows) == 3
            if hasattr(rows[0], "_mapping"):
                # First two should have updated quantity
                assert rows[0]._mapping.get("quantity") == 200
                assert rows[1]._mapping.get("quantity") == 200
                # Third should remain unchanged
                assert rows[2]._mapping.get("quantity") == 0
            else:
                assert rows[0][2] == 200
                assert rows[1][2] == 200
                assert rows[2][2] == 0

        finally:
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()
            session.close()

    def test_orm_table_delete_single_row(self, session_maker, conn):
        """Test DELETE using ORM table with raw SQL."""
        session = session_maker()
        try:
            # Clean up and insert test data
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()

            session.execute(
                text(
                    "INSERT INTO test_users_orm (id, name, email, age) VALUES "
                    "(1, 'Alice', 'alice@example.com', 30), "
                    "(2, 'Bob', 'bob@example.com', 25)"
                )
            )
            session.commit()

            # Delete one user
            session.execute(text("DELETE FROM test_users_orm WHERE id = 1"))
            session.commit()

            # Verify the delete
            result = session.execute(text("SELECT id, name FROM test_users_orm ORDER BY id"))
            rows = result.fetchall()

            assert len(rows) == 1
            if hasattr(rows[0], "_mapping"):
                assert rows[0]._mapping.get("id") == 2
                assert rows[0]._mapping.get("name") == "Bob"
            else:
                assert rows[0][0] == 2
                assert rows[0][1] == "Bob"

        finally:
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()
            session.close()

    def test_orm_table_delete_with_condition(self, session_maker, conn):
        """Test DELETE using ORM table with WHERE condition."""
        session = session_maker()
        try:
            # Clean up and insert test data
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()

            session.execute(
                text(
                    "INSERT INTO test_products_orm (id, name, price, quantity, in_stock) VALUES "
                    "(1, 'Widget', 19.99, 100, TRUE), "
                    "(2, 'Gadget', 29.99, 0, FALSE), "
                    "(3, 'Tool', 9.99, 5, TRUE)"
                )
            )
            session.commit()

            # Delete all out-of-stock products
            session.execute(text("DELETE FROM test_products_orm WHERE in_stock = FALSE"))
            session.commit()

            # Verify the delete
            result = session.execute(text("SELECT id, name, in_stock FROM test_products_orm ORDER BY id"))
            rows = result.fetchall()

            assert len(rows) == 2
            if hasattr(rows[0], "_mapping"):
                assert rows[0]._mapping.get("id") == 1
                assert rows[0]._mapping.get("in_stock") is True
                assert rows[1]._mapping.get("id") == 3
                assert rows[1]._mapping.get("in_stock") is True
            else:
                assert rows[0][0] == 1
                assert rows[0][2] is True
                assert rows[1][0] == 3
                assert rows[1][2] is True

        finally:
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()
            session.close()

    def test_orm_insert_single_object(self, session_maker, conn):
        """Test pure ORM INSERT using session.add() with a single object.

        Note: Uses text() for verification due to ORM query limitations with parameterized queries.
        """
        session = session_maker()
        try:
            # Clean up first
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()

            # Create and insert a user using pure ORM
            user = User()
            user.id = 100
            user.name = "John Doe"
            user.email = "john@example.com"
            user.age = 28
            user.is_active = True

            session.add(user)
            session.commit()

            # Verify the insert using raw SQL (ORM queries have parameter issues)
            result = session.execute(text("SELECT id, name, email, age, is_active FROM test_users_orm WHERE id = 100"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("id") == 100
                assert row._mapping.get("name") == "John Doe"
                assert row._mapping.get("email") == "john@example.com"
                assert row._mapping.get("age") == 28
                assert row._mapping.get("is_active") is True
            else:
                assert row[0] == 100
                assert row[1] == "John Doe"
                assert row[2] == "john@example.com"
                assert row[3] == 28
                assert row[4] is True

        finally:
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()
            session.close()

    def test_orm_insert_multiple_objects_individually(self, session_maker, conn):
        """Test pure ORM INSERT with multiple objects added individually.

        Note: Uses individual add() calls instead of add_all() due to executemany limitations.
        """
        session = session_maker()
        try:
            # Clean up first
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()

            # Create and insert products using pure ORM - add individually
            product1 = Product()
            product1.id = 200
            product1.name = "Laptop"
            product1.price = 999.99
            product1.quantity = 10
            product1.in_stock = True
            session.add(product1)
            session.commit()

            product2 = Product()
            product2.id = 201
            product2.name = "Mouse"
            product2.price = 25.50
            product2.quantity = 50
            product2.in_stock = True
            session.add(product2)
            session.commit()

            product3 = Product()
            product3.id = 202
            product3.name = "Keyboard"
            product3.price = 75.00
            product3.quantity = 0
            product3.in_stock = False
            session.add(product3)
            session.commit()

            # Verify the inserts
            result = session.execute(
                text("SELECT id, name, price, quantity, in_stock FROM test_products_orm ORDER BY id")
            )
            rows = result.fetchall()

            assert len(rows) == 3
            if hasattr(rows[0], "_mapping"):
                assert rows[0]._mapping.get("id") == 200
                assert rows[0]._mapping.get("name") == "Laptop"
                assert rows[0]._mapping.get("price") == 999.99
                assert rows[1]._mapping.get("id") == 201
                assert rows[1]._mapping.get("name") == "Mouse"
                assert rows[2]._mapping.get("id") == 202
                assert rows[2]._mapping.get("in_stock") is False
            else:
                assert rows[0][0] == 200
                assert rows[0][1] == "Laptop"
                assert rows[1][0] == 201
                assert rows[2][0] == 202

        finally:
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()
            session.close()

    def test_orm_update_via_raw_sql(self, session_maker, conn):
        """Test ORM model UPDATE using raw SQL.

        Note: Pure ORM update (query/modify/commit) not currently supported due to
        parameterized query limitations in the dialect.
        """
        session = session_maker()
        try:
            # Clean up and insert test data
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()

            # Create and insert initial user using ORM
            user = User()
            user.id = 300
            user.name = "Jane Smith"
            user.email = "jane@example.com"
            user.age = 25
            user.is_active = True
            session.add(user)
            session.commit()

            # Update using raw SQL (ORM query-based updates have parameterization issues)
            session.execute(
                text(
                    "UPDATE test_users_orm SET age = 26, email = 'jane.updated@example.com', is_active = FALSE WHERE id = 300"
                )
            )
            session.commit()

            # Verify the update
            result = session.execute(text("SELECT id, name, email, age, is_active FROM test_users_orm WHERE id = 300"))
            row = result.fetchone()

            assert row is not None
            if hasattr(row, "_mapping"):
                assert row._mapping.get("age") == 26
                assert row._mapping.get("email") == "jane.updated@example.com"
                assert row._mapping.get("is_active") is False
            else:
                assert row[3] == 26
                assert row[2] == "jane.updated@example.com"
                assert row[4] is False

        finally:
            session.execute(text("DELETE FROM test_users_orm"))
            session.commit()
            session.close()

    def test_orm_delete_via_raw_sql(self, session_maker, conn):
        """Test ORM model DELETE using raw SQL.

        Note: Pure ORM delete (query/delete/commit) not currently supported due to
        parameterized query limitations in the dialect.
        """
        session = session_maker()
        try:
            # Clean up and insert test data
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()

            # Create and insert products using ORM
            product1 = Product()
            product1.id = 400
            product1.name = "Monitor"
            product1.price = 299.99
            product1.quantity = 15
            product1.in_stock = True
            session.add(product1)
            session.commit()

            product2 = Product()
            product2.id = 401
            product2.name = "Speaker"
            product2.price = 49.99
            product2.quantity = 30
            product2.in_stock = True
            session.add(product2)
            session.commit()

            # Delete using raw SQL (ORM query-based deletes have parameterization issues)
            session.execute(text("DELETE FROM test_products_orm WHERE id = 400"))
            session.commit()

            # Verify the delete
            result = session.execute(text("SELECT id, name FROM test_products_orm ORDER BY id"))
            rows = result.fetchall()

            assert len(rows) == 1
            if hasattr(rows[0], "_mapping"):
                assert rows[0]._mapping.get("id") == 401
                assert rows[0]._mapping.get("name") == "Speaker"
            else:
                assert rows[0][0] == 401
                assert rows[0][1] == "Speaker"

        finally:
            session.execute(text("DELETE FROM test_products_orm"))
            session.commit()
            session.close()
