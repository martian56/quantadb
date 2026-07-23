# QuantaDB SQL syntax

The `quantadb-syntax` crate owns lexical and grammatical analysis only. Name
resolution, type checking, permissions, and execution belong to later layers.

## Current statements

- `CREATE TABLE [IF NOT EXISTS]`
- `CREATE [UNIQUE] INDEX [IF NOT EXISTS] ... ON ... (columns)`
- `DROP TABLE [IF EXISTS]`
- `DROP INDEX [IF EXISTS]`
- `BEGIN [TRANSACTION | WORK]` or `START TRANSACTION`
- `COMMIT [TRANSACTION | WORK]`
- `ROLLBACK [TRANSACTION | WORK]`
- `INSERT INTO ... [(columns)] VALUES ...`
- `SELECT ... FROM ... [WHERE ...] [ORDER BY key [ASC | DESC], ...] [LIMIT ...]`
- `UPDATE ... SET ... [WHERE ...]`
- `DELETE FROM ... [WHERE ...]`

Order keys are full expressions and do not have to appear in the
projection. Ascending order puts nulls last and descending puts them
first. `LIMIT` applies after the sort.

Supported column constraints are `PRIMARY KEY`, `NOT NULL`, `NULL`, and
`UNIQUE`. Current scalar types are `BOOL`, `INT`/`BIGINT`, `FLOAT`/`DOUBLE`,
`TEXT`, and `VARCHAR(n)`.

Expressions support:

- `OR`, `AND`, and unary `NOT`;
- `=`, `!=`, `<>`, `<`, `<=`, `>`, and `>=`;
- `IS NULL` and `IS NOT NULL`;
- `+`, `-`, `*`, `/`, and `%`;
- unary `+` and `-`;
- parentheses, strings, numbers, booleans, and `NULL`.

Unquoted identifiers are normalized to lowercase. Double quotes delimit
case-sensitive identifiers; single quotes delimit strings. Line comments use
`--`, and nested block comments use `/* ... */`.

## Diagnostics

Every token and AST node carries a half-open UTF-8 byte span into the source.
Syntax errors expose both a stable message and a span, allowing clients to
highlight the precise input range without parsing human-readable error text.

## Planned additions

Qualified names, joins, ordering, grouping, subqueries, common table
expressions, parameters, transaction isolation modes, and richer SQL types
will be added alongside binder and execution support.
