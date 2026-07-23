# QuantaSQL Specification v0.1 (legacy)

This document describes the original proof-of-concept grammar. It is retained
for historical reference and does not define the current QuantaDB syntax.

## Overview

QuantaSQL is a simplified SQL dialect designed for QuantaDB, focusing on essential database operations with a clean, minimal syntax.

## Core Features

### Data Types
- `INT` - 64-bit signed integer
- `TEXT` - Variable-length string
- `BOOL` - Boolean (true/false)
- `FLOAT` - 64-bit floating point number

### Table Operations

#### CREATE TABLE
```sql
CREATE TABLE table_name (
    column_name data_type,
    column_name data_type,
    ...
);
```

**Examples:**
```sql
CREATE TABLE users (id INT, name TEXT, age INT);
CREATE TABLE products (id INT, name TEXT, price FLOAT, in_stock BOOL);
```

#### DROP TABLE
```sql
DROP TABLE table_name;
```

**Example:**
```sql
DROP TABLE users;
```

### Data Manipulation

#### INSERT
```sql
INSERT INTO table_name VALUES (value1, value2, ...);
```

**Examples:**
```sql
INSERT INTO users VALUES (1, "Alice", 25);
INSERT INTO products VALUES (1, "Laptop", 999.99, true);
```

#### SELECT
```sql
SELECT column_list FROM table_name [WHERE condition];
```

**Examples:**
```sql
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 18;
SELECT id FROM products WHERE in_stock = true;
```

#### DELETE
```sql
DELETE FROM table_name [WHERE condition];
```

**Examples:**
```sql
DELETE FROM users WHERE id = 1;
DELETE FROM products WHERE in_stock = false;
```

### WHERE Clause Conditions

#### Comparison Operators
- `=` - Equal
- `!=` - Not equal
- `<` - Less than
- `>` - Greater than
- `<=` - Less than or equal
- `>=` - Greater than or equal

#### Logical Operators
- `AND` - Logical AND
- `OR` - Logical OR
- `NOT` - Logical NOT

**Examples:**
```sql
SELECT * FROM users WHERE age >= 18 AND age <= 65;
SELECT * FROM products WHERE price < 100 OR in_stock = true;
SELECT * FROM users WHERE NOT (age < 18);
```

### Literal Values

#### Integer Literals
```
123
-456
0
```

#### Text Literals
```
"Hello World"
'QuantaDB'
"User's Name"
```

#### Boolean Literals
```
true
false
```

#### Float Literals
```
3.14
-2.5
0.0
```

## Grammar (BNF-style)

```
statement ::= create_table | drop_table | insert | select | delete

create_table ::= "CREATE" "TABLE" identifier "(" column_def_list ")" ";"
drop_table ::= "DROP" "TABLE" identifier ";"

column_def_list ::= column_def ("," column_def)*
column_def ::= identifier data_type

data_type ::= "INT" | "TEXT" | "BOOL" | "FLOAT"

insert ::= "INSERT" "INTO" identifier "VALUES" "(" value_list ")" ";"
value_list ::= value ("," value)*

select ::= "SELECT" column_list "FROM" identifier [where_clause] ";"
column_list ::= "*" | identifier ("," identifier)*

delete ::= "DELETE" "FROM" identifier [where_clause] ";"

where_clause ::= "WHERE" condition
condition ::= expression comparison_op expression
            | condition "AND" condition
            | condition "OR" condition
            | "NOT" condition
            | "(" condition ")"

comparison_op ::= "=" | "!=" | "<" | ">" | "<=" | ">="

expression ::= identifier | literal
literal ::= integer | text | boolean | float

identifier ::= [a-zA-Z_][a-zA-Z0-9_]*
integer ::= ["-"]?[0-9]+
text ::= '"' [^"]* '"' | "'" [^']* "'"
boolean ::= "true" | "false"
float ::= ["-"]?[0-9]+"."?[0-9]*
```

## Implementation Notes

### Reserved Keywords
```
CREATE, TABLE, DROP, INSERT, INTO, VALUES, SELECT, FROM, WHERE, DELETE,
AND, OR, NOT, INT, TEXT, BOOL, FLOAT, true, false
```

### Case Sensitivity
- Keywords are case-insensitive
- Identifiers are case-sensitive
- String literals are case-sensitive

### Error Handling
The parser should provide clear error messages for:
- Syntax errors
- Type mismatches
- Unknown tables/columns
- Invalid operations

## Future Extensions (v0.2+)
- UPDATE statements
- JOIN operations
- Aggregate functions (COUNT, SUM, AVG, etc.)
- Indexes
- Transactions
- Constraints (PRIMARY KEY, NOT NULL, etc.)

## Example Queries

```sql
-- Create a simple user table
CREATE TABLE users (id INT, name TEXT, email TEXT, age INT);

-- Insert some users
INSERT INTO users VALUES (1, "Alice", "alice@example.com", 25);
INSERT INTO users VALUES (2, "Bob", "bob@example.com", 30);
INSERT INTO users VALUES (3, "Charlie", "charlie@example.com", 22);

-- Query users
SELECT * FROM users;
SELECT name, email FROM users WHERE age > 25;
SELECT * FROM users WHERE name = "Alice" OR age < 25;

-- Delete a user
DELETE FROM users WHERE id = 2;
```
