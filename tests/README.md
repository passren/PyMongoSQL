# Testing Guide for PyMongoSQL

This folder contains all the test files and utilities for PyMongoSQL.

## Test Types

### Unit Tests (with real MongoDB)
- `test_connection.py` - Connection class tests
- `test_cursor.py` - Cursor class tests  
- `test_result_set.py` - ResultSet class tests
- `test_sql_parser.py` - SQL parser tests

### Integration Tests (with real MongoDB)
- `test_integration_mongodb.py` - Tests with real MongoDB using TestContainers

## Test Utilities

### MongoDB Test Helper
```bash
# Start MongoDB container with test data
python mongo_test_helper.py start

# Check MongoDB status
python mongo_test_helper.py status

# Setup/reset test data
python mongo_test_helper.py setup

# Stop MongoDB container
python mongo_test_helper.py stop
```

### Docker Compose
```bash
# From tests directory
docker-compose -f docker-compose.test.yml up -d
docker-compose -f docker-compose.test.yml down
```



## Running Tests

### Quick Unit Tests (requires MongoDB)
```bash
cd ..
python -m pytest tests/test_connection.py tests/test_cursor.py tests/test_result_set.py -v
```

### Integration Tests (requires Docker)
```bash
cd ..
python -m pytest tests/test_integration_mongodb.py -v
```

### All Tests
```bash
cd ..
python -m pytest tests/ -v
```

## Test Database

The test MongoDB instance uses:
- Host: localhost
- Port: 27017  
- Database: test_db
- Collections: users, products
- No authentication required

Sample data is automatically loaded by the init script.