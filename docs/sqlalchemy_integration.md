# PyMongoSQL SQLAlchemy Integration

PyMongoSQL now includes a full SQLAlchemy dialect, enabling you to use MongoDB with SQLAlchemy's ORM and Core functionality through familiar SQL syntax.

## Version Compatibility

**Supported SQLAlchemy Versions:**
- ✅ SQLAlchemy 1.4.x (LTS)
- ✅ SQLAlchemy 2.0.x (Current)
- ✅ SQLAlchemy 2.1.x+ (Future)

The dialect automatically detects your SQLAlchemy version and adapts accordingly. Both 1.x and 2.x APIs are supported seamlessly.

## Quick Start

### Installation

```bash
# Install SQLAlchemy (1.4+ or 2.x)
pip install "sqlalchemy>=1.4.0,<3.0.0"

# PyMongoSQL already includes the dialect
```

### Version Detection

```python
import pymongosql

# Check SQLAlchemy support
print(f"SQLAlchemy installed: {pymongosql.__supports_sqlalchemy__}")
print(f"SQLAlchemy version: {pymongosql.__sqlalchemy_version__}")  
print(f"SQLAlchemy 2.x: {pymongosql.__supports_sqlalchemy_2x__}")

# Get compatibility info
from pymongosql.sqlalchemy_compat import check_sqlalchemy_compatibility
info = check_sqlalchemy_compatibility()
print(info['message'])
```

### Basic Usage (Version-Compatible)

```python
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker
import pymongosql

# Method 1: Use compatibility helpers (recommended)
from pymongosql.sqlalchemy_compat import get_base_class, create_pymongosql_engine

# Create engine with version-appropriate settings
engine = create_pymongosql_engine("pymongosql://localhost:27017/mydb")

# Get version-compatible base class
Base = get_base_class()

class User(Base):
    __tablename__ = 'users'
    
    id = Column('_id', String, primary_key=True)  # MongoDB's _id field
    username = Column(String, nullable=False)
    email = Column(String, nullable=False)
    age = Column(Integer)

# Create session with compatibility helper
SessionMaker = pymongosql.get_session_maker(engine)
session = SessionMaker()

# Use standard SQLAlchemy patterns (works with both 1.x and 2.x)
user = User(id="user123", username="john", email="john@example.com", age=30)
session.add(user)
session.commit()

# Query with ORM (syntax identical across versions)
users = session.query(User).filter(User.age >= 25).all()
```

### Manual Version Handling

```python
# Method 2: Manual version detection
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker
import pymongosql

# Check SQLAlchemy version
if pymongosql.__supports_sqlalchemy_2x__:
    # SQLAlchemy 2.x approach
    from sqlalchemy.orm import DeclarativeBase
    
    class Base(DeclarativeBase):
        pass
        
    engine = create_engine("pymongosql://localhost:27017/mydb", future=True)
else:
    # SQLAlchemy 1.x approach  
    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()
    
    engine = create_engine("pymongosql://localhost:27017/mydb")

# Model definition (identical for both versions)
class User(Base):
    __tablename__ = 'users'
    id = Column('_id', String, primary_key=True)
    username = Column(String, nullable=False)
    
# Rest of the code is version-agnostic
Session = sessionmaker(bind=engine)
session = Session()
```

## Features

### ✅ Supported SQLAlchemy Features

- **ORM Models**: Define models using `declarative_base()`
- **Core Expressions**: Use SQLAlchemy Core for query building
- **Sessions**: Full session management with commit/rollback
- **Relationships**: Basic relationship mapping
- **Query Building**: SQLAlchemy's query builder syntax
- **Raw SQL**: Execute raw SQL through `text()` objects
- **Connection Pooling**: Configurable connection pools
- **Transactions**: Basic transaction support where MongoDB allows

### 🔧 MongoDB-Specific Adaptations

- **Primary Keys**: Automatically maps to MongoDB's `_id` field
- **Collections**: SQL tables map to MongoDB collections
- **Documents**: SQL rows map to MongoDB documents
- **Schema-less**: Flexible schema handling for MongoDB's document nature
- **JSON Support**: Native handling of nested documents and arrays
- **Aggregation**: SQL GROUP BY translates to MongoDB aggregation pipelines

### ⚠️ Limitations

- **No Foreign Keys**: MongoDB doesn't enforce foreign key constraints
- **No ALTER TABLE**: Schema changes must be handled at application level
- **Limited Transactions**: Multi-document transactions have MongoDB limitations
- **No Sequences**: Auto-incrementing IDs must be handled manually

## URL Format

The PyMongoSQL dialect uses the following URL format:

```
pymongosql://[username:password@]host[:port]/database[?param1=value1&param2=value2]
```

### Examples

```python
# Basic connection
"pymongosql://localhost:27017/mydb"

# With authentication
"pymongosql://user:pass@localhost:27017/mydb"

# With MongoDB options
"pymongosql://localhost:27017/mydb?ssl=true&replicaSet=rs0"

# Using helper function
url = pymongosql.create_engine_url(
    host="mongo.example.com",
    port=27017,
    database="production",
    ssl=True,
    replicaSet="rs0"
)
```

## Advanced Usage

### Raw SQL Execution

```python
from sqlalchemy import text

# Execute raw SQL
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM users WHERE age > 25"))
    count = result.scalar()
```

### Aggregation Queries

```python
# SQL aggregation translates to MongoDB aggregation pipeline
from sqlalchemy import func

query = session.query(
    User.age,
    func.count(User.id).label('count')
).group_by(User.age).order_by(User.age)

results = query.all()
```

### JSON Document Operations

```python
# Query nested document fields (if supported by your SQL parser)
users_with_location = session.query(User).filter(
    text("profile->>'$.location' = 'New York'")
).all()
```

### Connection Configuration

```python
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

# Configure connection pool
engine = create_engine(
    "pymongosql://localhost:27017/mydb",
    poolclass=StaticPool,
    pool_size=5,
    max_overflow=10,
    echo=True  # Enable SQL logging
)
```

## Type Mapping

| SQL Type | MongoDB BSON Type | Notes |
|----------|-------------------|-------|
| VARCHAR, CHAR, TEXT | String | Text data |
| INTEGER | Int32 | 32-bit integers |
| BIGINT | Int64 | 64-bit integers |
| FLOAT, REAL | Double | Floating point |
| DECIMAL, NUMERIC | Decimal128 | High precision decimal |
| BOOLEAN | Boolean | True/false values |
| DATETIME, TIMESTAMP | Date | Date/time values |
| JSON | Object/Array | Nested documents |
| BINARY, BLOB | BinData | Binary data |

## Error Handling

```python
from pymongosql.error import DatabaseError, OperationalError

try:
    session.query(User).all()
except OperationalError as e:
    # Handle MongoDB connection errors
    print(f"Connection error: {e}")
except DatabaseError as e:
    # Handle query/data errors
    print(f"Database error: {e}")
```

## Migration from Raw PyMongoSQL

If you're already using PyMongoSQL directly, migrating to SQLAlchemy is straightforward:

### Before (Raw PyMongoSQL)
```python
import pymongosql

conn = pymongosql.connect("mongodb://localhost:27017/mydb")
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE age > 25")
results = cursor.fetchall()
```

### After (SQLAlchemy)
```python
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

engine = create_engine("pymongosql://localhost:27017/mydb")
Session = sessionmaker(bind=engine)
session = Session()

# Option 1: Raw SQL
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM users WHERE age > 25"))
    results = result.fetchall()

# Option 2: ORM
results = session.query(User).filter(User.age > 25).all()
```

## Best Practices

1. **Use _id for Primary Keys**: Always map your primary key to MongoDB's `_id` field
2. **Schema Design**: Design your models considering MongoDB's document nature
3. **Connection Pooling**: Configure appropriate pool sizes for your application
4. **Error Handling**: Implement proper error handling for MongoDB-specific issues
5. **Testing**: Use the provided test utilities for development

## Examples

See the `examples/sqlalchemy_integration.py` file for complete working examples and advanced usage patterns.

## Troubleshooting

### Common Issues

1. **"No dialect found"**: Ensure PyMongoSQL is properly installed and the dialect is registered
2. **Connection errors**: Verify MongoDB is running and accessible
3. **Schema issues**: Remember MongoDB is schema-less, some SQL patterns may not translate directly
4. **Performance**: Use indexes appropriately in MongoDB for optimal query performance

### Debug Mode

Enable SQL logging to see generated queries:

```python
engine = create_engine("pymongosql://localhost:27017/mydb", echo=True)
```

This will print all SQL statements and their MongoDB translations to the console.