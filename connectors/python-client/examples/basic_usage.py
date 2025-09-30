#!/usr/bin/env python3
"""
Basic usage example for QuantaDB Python client
"""

import quanta_python

def main():
    print("🔌 Connecting to QuantaDB...")
    
    client = quanta_python.QuantaClient("127.0.0.1:5432")
    client.connect()
    
    print("✅ Connected to QuantaDB!")
    
    # Create a table
    print("\n📝 Creating table 'users'...")
    result = client.execute("CREATE TABLE users (id INT, name TEXT, age INT)")
    print(f"Result: {result['message']}")
    
    # Insert some data
    print("\n➕ Inserting data...")
    result = client.execute('INSERT INTO users VALUES (1, "Alice", 25)')
    print(f"Result: {result['message']}")
    
    result = client.execute('INSERT INTO users VALUES (2, "Bob", 30)')
    print(f"Result: {result['message']}")
    
    result = client.execute('INSERT INTO users VALUES (3, "Charlie", 22)')
    print(f"Result: {result['message']}")
    
    # Query the data
    print("\n🔍 Querying all data...")
    result = client.execute("SELECT * FROM users")
    print(f"Result: {result['message']}")
    
    if result['data']:
        for i, row in enumerate(result['data']):
            print(f"Row {i + 1}: {row['values']}")
    
    # Query with WHERE clause
    print("\n🔍 Querying with WHERE clause...")
    result = client.execute("SELECT name FROM users WHERE age > 25")
    print(f"Result: {result['message']}")
    
    if result['data']:
        for i, row in enumerate(result['data']):
            print(f"Row {i + 1}: {row['values']}")
    
    # Delete a row
    print("\n🗑️ Deleting a row...")
    result = client.execute("DELETE FROM users WHERE id = 2")
    print(f"Result: {result['message']}")
    
    # Query again to see the change
    print("\n🔍 Querying after deletion...")
    result = client.execute("SELECT * FROM users")
    print(f"Result: {result['message']}")
    
    if result['data']:
        for i, row in enumerate(result['data']):
            print(f"Row {i + 1}: {row['values']}")
    
    client.disconnect()
    print("\n👋 Disconnected from QuantaDB")

if __name__ == "__main__":
    main()
