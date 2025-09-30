#!/usr/bin/env python3
"""
Simple test client for QuantaDB server
"""

import socket
import json
import time

def send_query(sock, query):
    """Send a query to the server and return the response"""
    # Send the query
    request = {"query": query}
    request_json = json.dumps(request) + "\n"
    sock.send(request_json.encode())
    
    # Receive the response
    response_data = sock.recv(4096).decode()
    response = json.loads(response_data.strip())
    
    return response

def test_quanta_server():
    """Test the QuantaDB server with various queries"""
    print("Testing QuantaDB Server...")
    
    try:
        # Connect to the server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", 54321))
        print("Connected to QuantaDB server")
        
        # Read welcome message
        welcome = sock.recv(4096).decode()
        print(f"Welcome: {welcome.strip()}")
        
        # Test queries
        test_queries = [
            "CREATE TABLE users (id INT, name TEXT, age INT)",
            'INSERT INTO users VALUES (1, "Alice", 25)',
            'INSERT INTO users VALUES (2, "Bob", 30)',
            "SELECT * FROM users",
            "SELECT name FROM users WHERE age > 25",
            'DELETE FROM users WHERE id = 2',
            "SELECT * FROM users",
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\nTest {i}: {query}")
            response = send_query(sock, query)
            
            if response["success"]:
                print(f"Success: {response['message']}")
                if response.get("data"):
                    print(f"Data: {response['data']}")
            else:
                print(f"Error: {response.get('error', 'Unknown error')}")
            
            time.sleep(0.1)  # Small delay between queries
        
        sock.close()
        print("\nAll tests completed!")
        
    except ConnectionRefusedError:
        print("Could not connect to QuantaDB server. Make sure it's running on 127.0.0.1:54321")
    except Exception as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    test_quanta_server()
