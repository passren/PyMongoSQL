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

- **Bridge SQL and NoSQL**: Provide SQL capabilities for MongoDB's nested document structures
- **Standard SQL Operations**: Support DQL (SELECT) and DML (INSERT, UPDATE, DELETE) operations with WHERE, ORDER BY, and LIMIT clauses
- **Seamless Integration**: Full compatibility with Python applications expecting DB API 2.0 compliance
- **Easy Migration**: Enable migration from traditional SQL databases to MongoDB without rewriting application code

## Features

- **DB API 2.0 Compliant**: Full compatibility with Python Database API 2.0 specification
- **PartiQL-based SQL Syntax**: Built on [PartiQL](https://partiql.org/tutorial.html) (SQL for semi-structured data), enabling seamless SQL querying of nested and hierarchical MongoDB documents
- **Nested Structure Support**: Query and filter deeply nested fields and arrays within MongoDB documents using standard SQL syntax
- **SQLAlchemy Integration**: Complete ORM and Core support with dedicated MongoDB dialect
- **SQL Query Support**: SELECT statements with WHERE conditions, field selection, and aliases
- **DML Support**: Full support for INSERT, UPDATE, and DELETE operations using PartiQL syntax
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
        print(f"Total users: {result[0]}")
```

### Using DictCursor for Dictionary Results

```python
from pymongosql import connect
from pymongosql.cursor import DictCursor

with connect(host="mongodb://localhost:27017/database") as conn:
    with conn.cursor(DictCursor) as cursor:
        cursor.execute('SELECT COUNT(*) as total FROM users')
        result = cursor.fetchone()
        print(f"Total users: {result['total']}")
```

### Cursor vs DictCursor

PyMongoSQL provides two cursor types for different result formats:

**Cursor** (default) - Returns results as tuples:
```python
cursor = connection.cursor()
cursor.execute('SELECT name, email FROM users')
row = cursor.fetchone()
print(row[0])  # Access by index
```

**DictCursor** - Returns results as dict:
```python
from pymongosql.cursor import DictCursor

cursor = connection.cursor(DictCursor)
cursor.execute('SELECT name, email FROM users')
row = cursor.fetchone()
print(row['name'])  # Access by column name
```

### Query with Parameters

PyMongoSQL supports two styles of parameterized queries for safe value substitution:

**Positional Parameters with ?**

```python
from pymongosql import connect

connection = connect(host="mongodb://localhost:27017/database")
cursor = connection.cursor()

cursor.execute(
    'SELECT name, email FROM users WHERE age > ? AND status = ?',
    [25, 'active']
)
```

**Named Parameters with :name**

```python
from pymongosql import connect

connection = connect(host="mongodb://localhost:27017/database")
cursor = connection.cursor()

cursor.execute(
    'SELECT name, email FROM users WHERE age > :age AND status = :status',
    {'age': 25, 'status': 'active'}
)
```

Parameters are substituted into the MongoDB filter during execution, providing protection against injection attacks.

## Supported SQL Features

### SELECT Statements

- **Field selection**: `SELECT name, age FROM users`
- **Wildcards**: `SELECT * FROM products`
- **Field aliases**: `SELECT name AS user_name, age AS user_age FROM users`
- **Nested fields**: `SELECT profile.name, profile.age FROM users`
- **Array access**: `SELECT items[0], items[1].name FROM orders`

### WHERE Clauses

- **Equality**: `WHERE name = 'John'`
- **Comparisons**: `WHERE age > 25`, `WHERE price <= 100.0`
- **Logical operators**: `WHERE age > 18 AND status = 'active'`, `WHERE age < 30 OR role = 'admin'`
- **Nested field filtering**: `WHERE profile.status = 'active'`
- **Array filtering**: `WHERE items[0].price > 100`

### Nested Field Support
- **Single-level**: `profile.name`, `settings.theme`
- **Multi-level**: `account.profile.name`, `config.database.host`
- **Array access**: `items[0].name`, `orders[1].total`
- **Complex queries**: `WHERE customer.profile.age > 18 AND orders[0].status = 'paid'`

> **Note**: Avoid SQL reserved words (`user`, `data`, `value`, `count`, etc.) as unquoted field names. Use alternatives or bracket notation for arrays.

### Sorting and Limiting

- **ORDER BY**: `ORDER BY name ASC, age DESC`
- **LIMIT**: `LIMIT 10`
- **Combined**: `ORDER BY created_at DESC LIMIT 5`

### INSERT Statements

PyMongoSQL supports inserting documents into MongoDB collections using PartiQL-style object and bag literals.

**Single Document**

```python
cursor.execute(
    "INSERT INTO Music {'title': 'Song A', 'artist': 'Alice', 'year': 2021}"
)
```

**Multiple Documents (Bag Syntax)**

```python
cursor.execute(
    "INSERT INTO Music << {'title': 'Song B', 'artist': 'Bob'}, {'title': 'Song C', 'artist': 'Charlie'} >>"
)
```

**Parameterized INSERT**

```python
# Positional parameters using ? placeholders
cursor.execute(
    "INSERT INTO Music {'title': ?, 'artist': ?, 'year': ?}",
    ["Song D", "Diana", 2020]
)
```

> **Note**: For parameterized INSERT, use positional parameters (`?`). Named placeholders (`:name`) are supported for SELECT, UPDATE, and DELETE queries.

### UPDATE Statements

PyMongoSQL supports updating documents in MongoDB collections using standard SQL UPDATE syntax.

**Update All Documents**

```python
cursor.execute("UPDATE Music SET available = false")
```

**Update with WHERE Clause**

```python
cursor.execute("UPDATE Music SET price = 14.99 WHERE year < 2020")
```

**Update Multiple Fields**

```python
cursor.execute(
    "UPDATE Music SET price = 19.99, available = true WHERE artist = 'Alice'"
)
```

**Update with Logical Operators**

```python
cursor.execute(
    "UPDATE Music SET price = 9.99 WHERE year = 2020 AND stock > 5"
)
```

**Parameterized UPDATE**

```python
# Positional parameters using ? placeholders
cursor.execute(
    "UPDATE Music SET price = ?, stock = ? WHERE artist = ?",
    [24.99, 50, "Bob"]
)
```

**Update Nested Fields**

```python
cursor.execute(
    "UPDATE Music SET details.publisher = 'XYZ Records' WHERE title = 'Song A'"
)
```

**Check Updated Row Count**

```python
cursor.execute("UPDATE Music SET available = false WHERE year = 2020")
print(f"Updated {cursor.rowcount} documents")
```

### DELETE Statements

PyMongoSQL supports deleting documents from MongoDB collections using standard SQL DELETE syntax.

**Delete All Documents**

```python
cursor.execute("DELETE FROM Music")
```

**Delete with WHERE Clause**

```python
cursor.execute("DELETE FROM Music WHERE year < 2020")
```

**Delete with Logical Operators**

```python
cursor.execute(
    "DELETE FROM Music WHERE year = 2019 AND available = false"
)
```

**Parameterized DELETE**

```python
# Positional parameters using ? placeholders
cursor.execute(
    "DELETE FROM Music WHERE artist = ? AND year < ?",
    ["Charlie", 2021]
)
```

**Check Deleted Row Count**

```python
cursor.execute("DELETE FROM Music WHERE available = false")
print(f"Deleted {cursor.rowcount} documents")
```

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

## Limitations & Roadmap

**Note**: PyMongoSQL currently supports DQL (Data Query Language) and DML (Data Manipulation Language) operations. The following SQL features are **not yet supported** but are planned for future releases:

- **DDL Operations** (Data Definition Language)  
  - `CREATE TABLE/COLLECTION`, `DROP TABLE/COLLECTION`
  - `CREATE INDEX`, `DROP INDEX`
  - `LIST TABLES/COLLECTIONS`
  - `ALTER TABLE/COLLECTION`
- **Advanced DML Operations**
  - `MERGE`, `UPSERT`
  - Transactions and multi-document operations

These features are on our development roadmap and contributions are welcome!

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

PyMongoSQL is distributed under the [MIT license](https://opensource.org/licenses/MIT).
