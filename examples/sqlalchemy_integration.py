#!/usr/bin/env python3
"""
Example usage of PyMongoSQL with SQLAlchemy.

This example demonstrates how to use PyMongoSQL as a SQLAlchemy dialect
to interact with MongoDB using familiar SQL syntax through SQLAlchemy's ORM.
"""

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, create_engine, text
from sqlalchemy.orm import sessionmaker

import pymongosql

# SQLAlchemy version detection for compatibility
try:
    import sqlalchemy

    SQLALCHEMY_2X = tuple(map(int, sqlalchemy.__version__.split(".")[:2])) >= (2, 0)
except ImportError:
    SQLALCHEMY_2X = False

# Create the base class for ORM models (version-compatible)
if SQLALCHEMY_2X:
    # SQLAlchemy 2.x style
    from sqlalchemy.orm import DeclarativeBase

    class Base(DeclarativeBase):
        pass

else:
    # SQLAlchemy 1.x style
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()


class User(Base):
    """Example User model for MongoDB collection."""

    __tablename__ = "users"

    # MongoDB always has _id as primary key
    id = Column("_id", String, primary_key=True)
    username = Column(String, nullable=False)
    email = Column(String, nullable=False)
    age = Column(Integer)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def main():
    """Demonstrate PyMongoSQL + SQLAlchemy usage."""
    print("🔗 PyMongoSQL + SQLAlchemy Integration Demo")
    print("=" * 50)

    # Method 1: Using the helper function
    print("\n1️⃣  Creating engine using helper function:")
    url = pymongosql.create_engine_url(host="localhost", port=27017, database="test_sqlalchemy", connect=True)
    print(f"   URL: {url}")

    # Method 2: Direct URL construction
    print("\n2️⃣  Creating engine using direct URL:")
    direct_url = "pymongosql://localhost:27017/test_sqlalchemy"
    print(f"   URL: {direct_url}")

    try:
        # Create SQLAlchemy engine
        engine = create_engine(url, echo=True)  # echo=True for SQL logging

        print("\n3️⃣  Testing basic connection:")
        with engine.connect() as conn:
            # Test raw SQL execution
            result = conn.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            print(f"   Connection test result: {row[0] if row else 'Failed'}")

        print("\n4️⃣  Creating session for ORM operations:")
        Session = sessionmaker(bind=engine)
        session = Session()

        # Create tables (collections in MongoDB)
        print("   Creating collections...")
        Base.metadata.create_all(engine)

        print("\n5️⃣  ORM Examples:")

        # Create a new user
        print("   Creating new user...")
        new_user = User(id="user123", username="john_doe", email="john@example.com", age=30, is_active=True)
        session.add(new_user)
        session.commit()
        print("   ✅ User created successfully")

        # Query users
        print("   Querying users...")
        users = session.query(User).filter(User.age >= 25).all()
        print(f"   Found {len(users)} users aged 25 or older")

        for user in users:
            print(f"   - {user.username} ({user.email}) - Age: {user.age}")

        # Update a user
        print("   Updating user...")
        user_to_update = session.query(User).filter(User.username == "john_doe").first()
        if user_to_update:
            user_to_update.age = 31
            session.commit()
            print("   ✅ User updated successfully")

        # Raw SQL through SQLAlchemy
        print("\n6️⃣  Raw SQL execution:")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) as user_count FROM users"))
            count_row = result.fetchone()
            if count_row:
                print(f"   Total users in collection: {count_row[0]}")

        session.close()
        print("\n🎉 Demo completed successfully!")

    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        print("   Make sure MongoDB is running and accessible")
        return 1

    return 0


def show_advanced_examples():
    """Show advanced SQLAlchemy features with PyMongoSQL."""
    print("\n" + "=" * 50)
    print("🚀 Advanced PyMongoSQL + SQLAlchemy Features")
    print("=" * 50)

    try:
        # Connection with advanced options
        url = pymongosql.create_engine_url(
            host="localhost", port=27017, database="advanced_test", maxPoolSize=10, retryWrites=True
        )

        engine = create_engine(url, pool_size=5, max_overflow=10)

        with engine.connect() as conn:
            # 1. Aggregation pipeline through SQL
            print("\n1️⃣  Aggregation through SQL:")
            agg_sql = text(
                """
                SELECT age, COUNT(*) as count 
                FROM users 
                GROUP BY age 
                ORDER BY age
            """
            )
            result = conn.execute(agg_sql)
            print("   Age distribution:")
            for row in result:
                print(f"   - Age {row[0]}: {row[1]} users")

            # 2. JSON operations (MongoDB documents)
            print("\n2️⃣  JSON document operations:")
            json_sql = text(
                """
                SELECT username, profile->>'$.location' as location
                FROM users 
                WHERE profile->>'$.location' IS NOT NULL
            """
            )
            result = conn.execute(json_sql)
            print("   Users with location data:")
            for row in result:
                print(f"   - {row[0]}: {row[1]}")

            # 3. Date range queries
            print("\n3️⃣  Date range queries:")
            date_sql = text(
                """
                SELECT username, created_at 
                FROM users 
                WHERE created_at >= DATE('2024-01-01')
                ORDER BY created_at DESC
            """
            )
            result = conn.execute(date_sql)
            print("   Recent users:")
            for row in result:
                print(f"   - {row[0]}: {row[1]}")

        print("\n✨ Advanced features demonstrated!")

    except Exception as e:
        print(f"\n❌ Advanced demo error: {e}")


if __name__ == "__main__":
    # Run basic demo
    exit_code = main()

    # Run advanced examples if basic demo succeeded
    if exit_code == 0:
        show_advanced_examples()

    print(f"\n📚 Integration Guide:")
    print("   1. Install: pip install sqlalchemy")
    print("   2. Import: from sqlalchemy import create_engine")
    print("   3. Connect: engine = create_engine('pymongosql://host:port/db')")
    print("   4. Use standard SQLAlchemy ORM and Core patterns")
    print("   5. Enjoy MongoDB with SQL syntax! 🎉")
