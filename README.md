# PyMongoSQL

[![PyPI](https://img.shields.io/pypi/v/pymongosql)](https://pypi.org/project/pymongosql/)
[![Test](https://github.com/passren/PyMongoSQL/actions/workflows/ci.yml/badge.svg)](https://github.com/passren/PyMongoSQL/actions/workflows/ci.yml)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/passren/PyMongoSQL/branch/main/graph/badge.svg?token=2CTRL80NP2)](https://codecov.io/gh/passren/PyMongoSQL)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://github.com/passren/PyMongoSQL/blob/0.1.2/LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0+-green.svg)](https://www.mongodb.com/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-1.4+_2.0+-darkgreen.svg)](https://www.sqlalchemy.org/)

PyMongoSQL is a Python [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) client for [MongoDB](https://www.mongodb.com/). It provides a familiar SQL interface to MongoDB, allowing developers to use SQL to interact with MongoDB collections.

## Objectives

PyMongoSQL implements the DB API 2.0 interfaces to provide SQL-like access to MongoDB. The project aims to:

- Bridge the gap between SQL and NoSQL by providing SQL capabilities for MongoDB
- Support standard SQL DQL (Data Query Language) operations including SELECT statements with WHERE, ORDER BY, and LIMIT clauses
- Provide seamless integration with existing Python applications that expect DB API 2.0 compliance
- Enable easy migration from traditional SQL databases to MongoDB

## Features

- **DB API 2.0 Compliant**: Full compatibility with Python Database API 2.0 specification
- **SQLAlchemy Integration**: Complete ORM and Core support with dedicated MongoDB dialect
- **SQL Query Support**: SELECT statements with WHERE conditions, field selection, and aliases
- **Connection String Support**: MongoDB URI format for easy configuration

## Requirements

- **Python**: 3.9, 3.10, 3.11, 3.12, 3.13+
- **MongoDB**: 7.0+

## Dependencies

- **PyMongo** (MongoDB Python Driver)
  - pymongo >= 4.15.0

- **ANTLR4** (SQL Parser Runtime)
  - antlr4-python3-runtime >= 4.13.0

### Optional Dependencies

- **SQLAlchemy** (for ORM/Core support)
  - sqlalchemy >= 1.4.0 (SQLAlchemy 1.4+ and 2.0+ supported)

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
    database="database"
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

with connect(host="mongodb://localhost:27017/database") as conn:
    with conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) as total FROM users')
        result = cursor.fetchone()
        print(f"Total users: {result['total']}")
```

### Query with Parameters

```python
from pymongosql import connect

connection = connect(host="mongodb://localhost:27017/database")
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

### WHERE Clauses
- Equality: `WHERE name = 'John'`
- Comparisons: `WHERE age > 25`, `WHERE price <= 100.0`
- Logical operators: `WHERE age > 18 AND status = 'active'`

### Sorting and Limiting
- ORDER BY: `ORDER BY name ASC, age DESC`
- LIMIT: `LIMIT 10`
- Combined: `ORDER BY created_at DESC LIMIT 5`

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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

PyMongoSQL is distributed under the [MIT license](https://opensource.org/licenses/MIT).
