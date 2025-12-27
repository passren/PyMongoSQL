# PyMongoSQL

[![PyPI](https://img.shields.io/pypi/v/pymongosql)](https://pypi.org/project/pymongosql/)
[![Test](https://github.com/passren/PyMongoSQL/actions/workflows/ci.yml/badge.svg)](https://github.com/passren/PyMongoSQL/actions/workflows/ci.yml)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/passren/PyMongoSQL/branch/main/graph/badge.svg?token=2CTRL80NP2)](https://codecov.io/gh/passren/PyMongoSQL)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://github.com/passren/PyMongoSQL/blob/0.1.2/LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0+-green.svg)](https://www.mongodb.com/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-1.4+_2.0+-darkgreen.svg)](https://www.sqlalchemy.org/)
[![Superset](https://img.shields.io/badge/Apache_Superset-1.0+-blue.svg)](https://superset.apache.org/docs/6.0.0/configuration/databases)

PyMongoSQL is a Python [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) client for [MongoDB](https://www.mongodb.com/). It provides a familiar SQL interface to MongoDB, allowing developers to use SQL to interact with MongoDB collections.

## Objectives

PyMongoSQL implements the DB API 2.0 interfaces to provide SQL-like access to MongoDB, built on PartiQL syntax for querying semi-structured data. The project aims to:

- Bridge the gap between SQL and NoSQL by providing SQL capabilities for MongoDB's nested document structures
- Support standard SQL DQL (Data Query Language) operations including SELECT statements with WHERE, ORDER BY, and LIMIT clauses on nested and hierarchical data
- Provide seamless integration with existing Python applications that expect DB API 2.0 compliance
- Enable easy migration from traditional SQL databases to MongoDB without rewriting queries for document traversal

## Features

- **DB API 2.0 Compliant**: Full compatibility with Python Database API 2.0 specification
- **PartiQL-based SQL Syntax**: Built on [PartiQL](https://partiql.org/tutorial.html) (SQL for semi-structured data), enabling seamless SQL querying of nested and hierarchical MongoDB documents
- **Nested Structure Support**: Query and filter deeply nested fields and arrays within MongoDB documents using standard SQL syntax
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

- **JMESPath** (JSON/Dict Path Query)
  - jmespath >= 1.0.0

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
- **Field aliases**: `SELECT name as user_name, age as user_age FROM users`
- **Nested fields**: `SELECT profile.name, profile.age FROM users`
- **Array access**: `SELECT items[0], items[1].name FROM orders`

### WHERE Clauses
- Equality: `WHERE name = 'John'`
- Comparisons: `WHERE age > 25`, `WHERE price <= 100.0`
- Logical operators: `WHERE age > 18 AND status = 'active'`
- **Nested field filtering**: `WHERE profile.status = 'active'`
- **Array filtering**: `WHERE items[0].price > 100`

### Nested Field Support
- **Single-level**: `profile.name`, `settings.theme`
- **Multi-level**: `account.profile.name`, `config.database.host`
- **Array access**: `items[0].name`, `orders[1].total`
- **Complex queries**: `WHERE customer.profile.age > 18 AND orders[0].status = 'paid'`

> **Note**: Avoid SQL reserved words (`user`, `data`, `value`, `count`, etc.) as unquoted field names. Use alternatives or bracket notation for arrays.

### Sorting and Limiting
- ORDER BY: `ORDER BY name ASC, age DESC`
- LIMIT: `LIMIT 10`
- Combined: `ORDER BY created_at DESC LIMIT 5`

## Limitations & Roadmap

**Note**: Currently PyMongoSQL focuses on Data Query Language (DQL) operations. The following SQL features are **not yet supported** but are planned for future releases:

- **DML Operations** (Data Manipulation Language)
  - `INSERT`, `UPDATE`, `DELETE`
- **DDL Operations** (Data Definition Language)  
  - `CREATE TABLE/COLLECTION`, `DROP TABLE/COLLECTION`
  - `CREATE INDEX`, `DROP INDEX`
  - `LIST TABLES/COLLECTIONS`

These features are on our development roadmap and contributions are welcome!

## Apache Superset Integration

PyMongoSQL can be used as a database driver in Apache Superset for querying and visualizing MongoDB data:

1. **Install PyMongoSQL**: Install PyMongoSQL on the Superset app server:
   ```bash
   pip install pymongosql
   ```
2. **Create Connection**: Connect to your MongoDB instance using the connection URI with superset mode:
   ```
   mongodb://username:password@host:port/database?mode=superset
   ```
   or for MongoDB Atlas:
   ```
   mongodb+srv://username:password@host/database?mode=superset
   ```
3. **Use SQL Lab**: Write and execute SQL queries against MongoDB collections directly in Superset's SQL Lab
4. **Create Visualizations**: Build charts and dashboards from your MongoDB queries using Superset's visualization tools

This allows seamless integration between MongoDB data and Superset's BI capabilities without requiring data migration to traditional SQL databases.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

PyMongoSQL is distributed under the [MIT license](https://opensource.org/licenses/MIT).
