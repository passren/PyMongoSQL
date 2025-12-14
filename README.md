# PyMongoSQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![MongoDB](https://img.shields.io/badge/MongoDB-4.0+-green.svg)](https://www.mongodb.com/)

PyMongoSQL is a Python [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) client for [MongoDB](https://www.mongodb.com/). It provides a familiar SQL interface to MongoDB, allowing developers to use SQL queries to interact with MongoDB collections.

## Objectives

PyMongoSQL implements the DB API 2.0 interfaces to provide SQL-like access to MongoDB. The project aims to:

- Bridge the gap between SQL and NoSQL by providing SQL query capabilities for MongoDB
- Support standard SQL DQL (Data Query Language) operations including SELECT statements with WHERE, ORDER BY, and LIMIT clauses
- Provide seamless integration with existing Python applications that expect DB API 2.0 compliance
- Enable easy migration from traditional SQL databases to MongoDB
- Support field aliasing and projection mapping for flexible result set handling
- Maintain high performance through direct `db.command()` execution instead of high-level APIs

## Features

- **DB API 2.0 Compliant**: Full compatibility with Python Database API 2.0 specification
- **SQL Query Support**: SELECT statements with WHERE conditions, field selection, and aliases
- **MongoDB Native Integration**: Direct `db.command()` execution for optimal performance
- **Connection String Support**: MongoDB URI format for easy configuration
- **Result Set Handling**: Support for `fetchone()`, `fetchmany()`, and `fetchall()` operations
- **Field Aliasing**: SQL-style field aliases with automatic projection mapping
- **Context Manager Support**: Automatic resource management with `with` statements
- **Transaction Ready**: Architecture designed for future DML operation support (INSERT, UPDATE, DELETE)

## Requirements

- **Python**: 3.9, 3.10, 3.11, 3.12, 3.13+
- **MongoDB**: 4.0+

## Dependencies

- **PyMongo** (MongoDB Python Driver)
  - pymongo >= 4.15.0

- **ANTLR4** (SQL Parser Runtime)
  - antlr4-python3-runtime >= 4.13.0

## Installation

```bash
pip install pymongosql
```

Or install from source:

```bash
git clone https://github.com/your-username/PyMongoSQL.git
cd PyMongoSQL
pip install -e .
```

## Quick Start

### Basic Usage

```python
from pymongosql import connect

# Connect to MongoDB
connection = connect(
    host="mongodb://localhost:27017",
    database="test_db"
)

cursor = connection.cursor()
cursor.execute('SELECT name, email FROM users WHERE age > 25')
print(cursor.fetchall())
```

### Using Connection String

```python
from pymongosql import connect

# Connect with authentication
connection = connect(
    host="mongodb://username:password@localhost:27017/database?authSource=admin"
)

cursor = connection.cursor()
cursor.execute('SELECT * FROM products WHERE category = ?', ['Electronics'])

for row in cursor:
    print(row)
```

### Context Manager Support

```python
from pymongosql import connect

with connect(host="mongodb://localhost:27017", database="mydb") as conn:
    with conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) as total FROM users')
        result = cursor.fetchone()
        print(f"Total users: {result['total']}")
```

### Field Aliases and Projections

```python
from pymongosql import connect

connection = connect(host="mongodb://localhost:27017", database="ecommerce")
cursor = connection.cursor()

# Use field aliases for cleaner result sets
cursor.execute('''
    SELECT 
        name AS product_name,
        price AS cost,
        category AS product_type
    FROM products 
    WHERE in_stock = true
    ORDER BY price DESC
    LIMIT 10
''')

products = cursor.fetchall()
for product in products:
    print(f"{product['product_name']}: ${product['cost']}")
```

### Query with Parameters

```python
from pymongosql import connect

connection = connect(host="mongodb://localhost:27017", database="blog")
cursor = connection.cursor()

# Parameterized queries for security
min_age = 18
status = 'active'

cursor.execute('''
    SELECT name, email, created_at 
    FROM users 
    WHERE age >= ? AND status = ?
''', [min_age, status])

users = cursor.fetchmany(5)  # Fetch first 5 results
while users:
    for user in users:
        print(f"User: {user['name']} ({user['email']})")
    users = cursor.fetchmany(5)  # Fetch next 5
```

## Supported SQL Features

### SELECT Statements
- Field selection: `SELECT name, age FROM users`
- Wildcards: `SELECT * FROM products`
- Field aliases: `SELECT name AS user_name, age AS user_age FROM users`

### WHERE Clauses
- Equality: `WHERE name = 'John'`
- Comparisons: `WHERE age > 25`, `WHERE price <= 100.0`
- Logical operators: `WHERE age > 18 AND status = 'active'`

### Sorting and Limiting
- ORDER BY: `ORDER BY name ASC, age DESC`
- LIMIT: `LIMIT 10`
- Combined: `ORDER BY created_at DESC LIMIT 5`

## Architecture

PyMongoSQL uses a multi-layer architecture:

1. **SQL Parser**: Built with ANTLR4 for robust SQL parsing
2. **Query Planner**: Converts SQL AST to MongoDB query plans
3. **Command Executor**: Direct `db.command()` execution for performance
4. **Result Processor**: Handles projection mapping and result set iteration

## Connection Options

```python
from pymongosql.connection import Connection

# Basic connection
conn = Connection(host="localhost", port=27017, database="mydb")

# With authentication
conn = Connection(
    host="mongodb://user:pass@host:port/db?authSource=admin",
    database="mydb"
)

# Connection properties
print(conn.host)           # MongoDB connection URL
print(conn.port)           # Port number
print(conn.database_name)  # Database name
print(conn.is_connected)   # Connection status
```

## Error Handling

```python
from pymongosql import connect
from pymongosql.error import ProgrammingError, SqlSyntaxError

try:
    connection = connect(host="mongodb://localhost:27017", database="test")
    cursor = connection.cursor()
    cursor.execute("INVALID SQL SYNTAX")
except SqlSyntaxError as e:
    print(f"SQL syntax error: {e}")
except ProgrammingError as e:
    print(f"Programming error: {e}")
```

## Development Status

PyMongoSQL is currently focused on DQL (Data Query Language) operations. Future releases will include:

- **DML Operations**: INSERT, UPDATE, DELETE statements
- **Advanced SQL Features**: JOINs, subqueries, aggregations
- **Schema Operations**: CREATE/DROP collection commands
- **Transaction Support**: Multi-document ACID transactions

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

PyMongoSQL is distributed under the [MIT license](https://opensource.org/licenses/MIT).
