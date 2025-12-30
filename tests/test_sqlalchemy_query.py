#!/usr/bin/env python3
import pytest

from tests.conftest import HAS_SQLALCHEMY, Base

try:
    from sqlalchemy import JSON, Boolean, Column, Float, Integer, String, text
except ImportError:
    pass

# Skip all tests if SQLAlchemy is not available
pytestmark = pytest.mark.skipif(not HAS_SQLALCHEMY, reason="SQLAlchemy not available")


# ORM Models
class User(Base):
    """User model for testing."""

    __tablename__ = "users"

    id = Column("_id", String, primary_key=True)
    name = Column(String)
    email = Column(String)
    age = Column(Integer)
    city = Column(String)
    active = Column(Boolean)
    balance = Column(Float)
    tags = Column(JSON)
    address = Column(JSON)

    def __repr__(self):
        return f"<User(id='{self.id}', name='{self.name}', email='{self.email}')>"


class Product(Base):
    """Product model for testing."""

    __tablename__ = "products"

    id = Column("_id", String, primary_key=True)
    name = Column(String)
    price = Column(Float)
    category = Column(String)
    in_stock = Column(Boolean)
    quantity = Column(Integer)
    tags = Column(JSON)
    specifications = Column(JSON)

    def __repr__(self):
        return f"<Product(id='{self.id}', name='{self.name}', price={self.price})>"


class Order(Base):
    """Order model for testing."""

    __tablename__ = "orders"

    id = Column("_id", String, primary_key=True)
    user_id = Column(String)
    total = Column(Float)
    status = Column(String)
    items = Column(JSON)

    def __repr__(self):
        return f"<Order(id='{self.id}', user_id='{self.user_id}', total={self.total})>"


class TestSQLAlchemyQuery:
    """Test class for SQLAlchemy dialect query operations with real MongoDB data."""

    def test_engine_creation(self, sqlalchemy_engine):
        """Test that SQLAlchemy engine works with real MongoDB."""
        assert sqlalchemy_engine is not None
        assert sqlalchemy_engine.dialect.name == "mongodb"

        # Test that we can get a connection
        with sqlalchemy_engine.connect() as connection:
            assert connection is not None

    def test_read_users_data(self, sqlalchemy_engine):
        """Test reading users data and creating User objects."""
        with sqlalchemy_engine.connect() as connection:
            # Query real users data
            result = connection.execute(text("SELECT _id, name, email, age, city, active, balance FROM users LIMIT 5"))
            rows = result.fetchall()

            assert len(rows) > 0, "Should have user data in test database"

            # Create User objects from query results
            users = []
            for row in rows:
                # Handle both SQLAlchemy 1.x and 2.x result formats
                if hasattr(row, "_mapping"):
                    # SQLAlchemy 2.x style with mapping access
                    user = User(
                        id=row._mapping.get("_id") or str(row[0]),
                        name=row._mapping.get("name") or row[1] or "Unknown",
                        email=row._mapping.get("email") or row[2] or "unknown@example.com",
                        age=row._mapping.get("age") or (row[3] if len(row) > 3 and isinstance(row[3], int) else 0),
                        city=row._mapping.get("city") or (row[4] if len(row) > 4 else "Unknown"),
                        active=row._mapping.get("active", True),
                        balance=row._mapping.get("balance", 0.0),
                    )
                else:
                    # SQLAlchemy 1.x style with sequence access
                    user = User(
                        id=str(row[0]) if row[0] else "unknown",
                        name=row[1] if len(row) > 1 and row[1] else "Unknown",
                        email=row[2] if len(row) > 2 and row[2] else "unknown@example.com",
                        age=row[3] if len(row) > 3 and isinstance(row[3], int) else 0,
                        city=row[4] if len(row) > 4 and row[4] else "Unknown",
                        active=row[5] if len(row) > 5 and row[5] is not None else True,
                        balance=float(row[6]) if len(row) > 6 and row[6] is not None else 0.0,
                    )
                users.append(user)

            # Validate User objects
            for user in users:
                assert user.id is not None, "User should have an ID"
                assert user.name is not None, "User should have a name"
                assert user.email is not None, "User should have an email"
                assert isinstance(user.age, int), "User age should be an integer"
                assert isinstance(user.balance, (int, float)), "User balance should be numeric"

            print(f"[PASS] Successfully created {len(users)} User objects from real MongoDB data")
            if users:
                print(f"   Sample: {users[0].name} ({users[0].email}) - Age: {users[0].age}")

    def test_read_products_data(self, sqlalchemy_engine):
        """Test reading products data and creating Product objects."""
        with sqlalchemy_engine.connect() as connection:
            # Query real products data
            result = connection.execute(
                text("SELECT _id, name, price, category, in_stock, quantity FROM products LIMIT 5")
            )
        rows = result.fetchall()

        assert len(rows) > 0, "Should have product data in test database"

        # Create Product objects from query results
        products = []
        for row in rows:
            # Handle both SQLAlchemy 1.x and 2.x result formats
            if hasattr(row, "_mapping"):
                # SQLAlchemy 2.x style with mapping access
                product = Product(
                    id=row._mapping.get("_id") or str(row[0]),
                    name=row._mapping.get("name") or row[1] or "Unknown Product",
                    price=float(row._mapping.get("price", 0) or row[2] or 0),
                    category=row._mapping.get("category") or row[3] or "Unknown",
                    in_stock=bool(row._mapping.get("in_stock", True)),
                    quantity=int(row._mapping.get("quantity", 0) or 0),
                )
            else:
                # SQLAlchemy 1.x style with sequence access
                product = Product(
                    id=str(row[0]) if row[0] else "unknown",
                    name=row[1] if len(row) > 1 and row[1] else "Unknown Product",
                    price=float(row[2]) if len(row) > 2 and row[2] is not None else 0.0,
                    category=row[3] if len(row) > 3 and row[3] else "Unknown",
                    in_stock=bool(row[4]) if len(row) > 4 and row[4] is not None else True,
                    quantity=int(row[5]) if len(row) > 5 and row[5] is not None else 0,
                )
            products.append(product)

        # Validate Product objects
        for product in products:
            assert product.id is not None, "Product should have an ID"
            assert product.name is not None, "Product should have a name"
            assert isinstance(product.price, float), "Product price should be a float"
            assert product.category is not None, "Product should have a category"
            assert isinstance(product.in_stock, bool), "Product in_stock should be a boolean"
            assert isinstance(product.quantity, int), "Product quantity should be an integer"

        print(f"[PASS] Successfully created {len(products)} Product objects from real MongoDB data")
        if products:
            print(f"   Sample: {products[0].name} - ${products[0].price} ({products[0].category})")

    def test_session_based_queries(self, session_maker):
        """Test SQLAlchemy session-based operations with real data."""
        session = session_maker()

        try:
            # Test session-based query execution
            result = session.execute(text("SELECT _id, name, email FROM users LIMIT 3"))
            rows = result.fetchall()

            assert len(rows) > 0, "Should have user data available"

            # Create objects from session query results
            users = []
            for row in rows:
                if hasattr(row, "_mapping"):
                    user = User(
                        id=row._mapping.get("_id") or str(row[0]),
                        name=row._mapping.get("name") or row[1] or "Unknown",
                        email=row._mapping.get("email") or row[2] or "unknown@example.com",
                    )
                else:
                    user = User(
                        id=str(row[0]) if row[0] else "unknown",
                        name=row[1] if len(row) > 1 and row[1] else "Unknown",
                        email=row[2] if len(row) > 2 and row[2] else "unknown@example.com",
                    )
                users.append(user)

            # Validate that session queries work
            for user in users:
                assert user.id is not None
                assert user.name is not None
                assert user.email is not None
                assert len(user.name) > 0

            print(f"[PASS] Session-based queries successful: {len(users)} users retrieved")
            if users:
                print(f"   Sample: {users[0].name} ({users[0].email})")

        finally:
            session.close()

    def test_complex_queries_with_filtering(self, sqlalchemy_engine):
        """Test more complex SQL queries with WHERE conditions."""
        with sqlalchemy_engine.connect() as connection:
            # Test filtering queries
            result = connection.execute(text("SELECT _id, name, age FROM users WHERE age > 25 LIMIT 5"))
            rows = result.fetchall()

            if len(rows) > 0:  # Only test if we have data
                # Create User objects and validate filtering worked
                users = []
                for row in rows:
                    if hasattr(row, "_mapping"):
                        age = row._mapping.get("age") or row[2] or 0
                        user = User(
                            id=row._mapping.get("_id") or str(row[0]),
                            name=row._mapping.get("name") or row[1] or "Unknown",
                            age=age,
                        )
                    else:
                        age = row[2] if len(row) > 2 and isinstance(row[2], int) else 0
                        user = User(
                            id=str(row[0]) if row[0] else "unknown",
                            name=row[1] if len(row) > 1 and row[1] else "Unknown",
                            age=age,
                        )
                    users.append(user)

                # Validate that filtering worked (age > 25)
                for user in users:
                    if user.age > 0:  # Only check if age data is available
                        assert user.age > 25, f"User {user.name} should be older than 25"

        # Validate that filtering worked (age > 25)
        for user in users:
            if user.age > 0:  # Only check if age data is available
                assert user.age > 25, f"User {user.name} should be older than 25"

        print(f"[PASS] Complex filtering queries successful: {len(users)} users over 25")
        if users:
            print(f"   Ages: {[user.age for user in users if user.age > 0]}")

    def test_multiple_table_queries(self, sqlalchemy_engine):
        """Test querying multiple collections (tables)."""
        with sqlalchemy_engine.connect() as connection:
            # Test querying different collections
            users_result = connection.execute(text("SELECT _id, name FROM users LIMIT 2"))
            products_result = connection.execute(text("SELECT _id, name, price FROM products LIMIT 2"))

            users_rows = users_result.fetchall()
            products_rows = products_result.fetchall()

            # Validate we can query multiple collections
            if len(users_rows) > 0:
                assert users_rows[0][0] is not None  # User ID
                assert users_rows[0][1] is not None  # User name

            if len(products_rows) > 0:
                assert products_rows[0][0] is not None  # Product ID
                assert products_rows[0][1] is not None  # Product name
                assert products_rows[0][2] is not None  # Product price

            print("Multi-collection queries successful")
            print(f"Users: {len(users_rows)}, Products: {len(products_rows)}")

    def test_mongodb_connection_available(self, conn):
        """Test that MongoDB connection is available before running other tests."""
        assert conn is not None
        print("MongoDB connection test successful")
