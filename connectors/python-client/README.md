# QuantaDB Python Client

Python client library for QuantaDB - a modern, fast database management system.

## Installation

```bash
pip install quanta-python
```

## Quick Start

```python
import quanta_python

# Connect to QuantaDB server
client = quanta_python.QuantaClient("127.0.0.1:5432")
client.connect()

# Create a table
result = client.execute("CREATE TABLE users (id INT, name TEXT, age INT)")
print(result["message"])

# Insert data
result = client.execute('INSERT INTO users VALUES (1, "Alice", 25)')
print(result["message"])

# Query data
result = client.execute("SELECT * FROM users")
print(result["message"])

if result["data"]:
    for row in result["data"]:
        print(f"Row: {row['values']}")

# Disconnect
client.disconnect()
```

## API Reference

### QuantaClient

#### `__init__(address: str)`
Create a new QuantaDB client.

- `address`: Server address in format "host:port"

#### `connect()`
Connect to the QuantaDB server.

#### `execute(query: str) -> dict`
Execute a SQL query and return the result.

Returns a dictionary with:
- `success`: Boolean indicating if the query succeeded
- `message`: Human-readable message
- `data`: List of rows (for SELECT queries)
- `error`: Error message (if query failed)

#### `disconnect()`
Disconnect from the server.

#### `is_connected() -> bool`
Check if the client is connected to the server.

## Supported SQL

QuantaDB supports a subset of SQL operations:

- `CREATE TABLE`
- `DROP TABLE`
- `INSERT INTO ... VALUES`
- `SELECT ... FROM ... WHERE`
- `DELETE FROM ... WHERE`

See the [QuantaSQL specification](../docs/quanta-sql-spec-v0.1.md) for details.
