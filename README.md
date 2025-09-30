# QuantaDB 🚀

A modern, fast database management system built with Rust, featuring a custom SQL dialect (QuantaSQL) and multiple client interfaces.

## Features

- **Custom SQL Language (QuantaSQL)**: Simplified SQL dialect focusing on essential operations
- **High Performance**: Built with Rust for speed and memory safety
- **Multiple Client Interfaces**: Rust, Python, and Desktop clients
- **Real-time Query Execution**: TCP-based protocol for fast communication
- **Cross-platform**: Works on Windows, macOS, and Linux

## Architecture

```
QuantaDB/
├── server/          # Core database server
├── connectors/      # Client libraries
│   ├── rust-client/ # Rust client library
│   └── python-client/ # Python client library
├── client/          # Desktop client (Tauri + Vue.js)
├── client-web/      # Web interface for desktop client
└── docs/           # Documentation
```

## Quick Start

### 1. Start the Server

```bash
cd server
cargo run
```

The server will start on `127.0.0.1:5432`.

### 2. Use the Desktop Client

```bash
cd client
cargo tauri dev
```

### 3. Use the Rust Client

```bash
cd connectors/rust-client
cargo run --example basic_usage
```

### 4. Use the Python Client

```bash
cd connectors/python-client
pip install -e .
python examples/basic_usage.py
```

## QuantaSQL Examples

```sql
-- Create a table
CREATE TABLE users (id INT, name TEXT, age INT);

-- Insert data
INSERT INTO users VALUES (1, "Alice", 25);
INSERT INTO users VALUES (2, "Bob", 30);

-- Query data
SELECT * FROM users;
SELECT name FROM users WHERE age > 25;

-- Delete data
DELETE FROM users WHERE id = 1;
```

## Supported SQL Operations

- `CREATE TABLE` - Create new tables
- `DROP TABLE` - Remove tables
- `INSERT INTO ... VALUES` - Insert data
- `SELECT ... FROM ... WHERE` - Query data
- `DELETE FROM ... WHERE` - Delete data

## Data Types

- `INT` - 64-bit signed integer
- `TEXT` - Variable-length string
- `BOOL` - Boolean (true/false)
- `FLOAT` - 64-bit floating point number

## Development

### Building the Server

```bash
cd server
cargo build --release
```

### Building the Desktop Client

```bash
cd client
cargo tauri build
```

### Building the Python Client

```bash
cd connectors/python-client
maturin develop
```

## Project Status

This is a proof-of-concept implementation demonstrating:

- ✅ Custom SQL parser and AST
- ✅ In-memory storage engine
- ✅ TCP server with custom protocol
- ✅ Rust client library
- ✅ Python client library (PyO3)
- ✅ Desktop client (Tauri + Vue.js)
- ✅ Basic CRUD operations
- ✅ WHERE clause filtering
- ✅ Type system with validation

## Future Roadmap

- [ ] Persistent storage (disk-based)
- [ ] Indexes for faster queries
- [ ] Transactions and ACID properties
- [ ] JOIN operations
- [ ] Aggregate functions
- [ ] Network clustering
- [ ] Query optimization
- [ ] Backup and recovery

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- Built with [Rust](https://rust-lang.org/)
- SQL parsing with [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)
- Desktop client with [Tauri](https://tauri.app/)
- Web interface with [Vue.js](https://vuejs.org/)
- Python bindings with [PyO3](https://pyo3.rs/)
