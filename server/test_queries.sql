-- QuantaDB Test Queries
-- These queries test the basic functionality of QuantaDB

-- Create a test table
CREATE TABLE users (id INT, name TEXT, age INT);

-- Insert some test data
INSERT INTO users VALUES (1, "Alice", 25);
INSERT INTO users VALUES (2, "Bob", 30);
INSERT INTO users VALUES (3, "Charlie", 22);

-- Query all data
SELECT * FROM users;

-- Query with WHERE clause
SELECT name FROM users WHERE age > 25;

-- Query with multiple conditions
SELECT * FROM users WHERE age >= 25 AND name = "Alice";

-- Delete a record
DELETE FROM users WHERE id = 2;

-- Query after deletion
SELECT * FROM users;

-- Create another table
CREATE TABLE products (id INT, name TEXT, price FLOAT, in_stock BOOL);

-- Insert product data
INSERT INTO products VALUES (1, "Laptop", 999.99, true);
INSERT INTO products VALUES (2, "Mouse", 29.99, false);

-- Query products
SELECT * FROM products;

-- Query with boolean condition
SELECT name FROM products WHERE in_stock = true;

-- Drop a table
DROP TABLE products;
