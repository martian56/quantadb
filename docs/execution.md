# QuantaDB relational execution

The `quantadb-engine` crate maps relational schemas and rows onto durable MVCC
keys. The network protocol is deliberately kept outside this crate.

## Durable records

Catalog entries, table-ID allocation, rows, and unique-value ownership all live
in the same MVCC keyspace. Schema and row payloads carry explicit format
versions. Table names and row identities use length-delimited binary keys, so
component boundaries are unambiguous.

Tables without an explicit primary key receive a durable hidden row ID.
Explicit integer, float, boolean, and text primary keys use order-preserving
binary encodings. `UNIQUE` columns additionally own synthetic MVCC keys. Two
snapshot transactions attempting the same unique value therefore conflict at
commit even when their physical row keys differ. Unique keys store the owning
physical row key and are updated atomically when a primary key changes.

## Transactions

Every TCP connection owns one engine session. `BEGIN`, subsequent DDL/DML
frames, and `COMMIT` or `ROLLBACK` operate on the same MVCC transaction.
Statements outside an explicit transaction use autocommit. An execution error
inside an explicit transaction aborts it until `ROLLBACK`.

DDL, rows, hidden-ID allocation, and unique ownership are committed atomically.
Uncommitted catalog and row changes are invisible to other sessions.

## Current execution surface

- `CREATE TABLE` and `DROP TABLE`
- multi-row `INSERT`
- `SELECT` projections, expressions, `WHERE`, and `LIMIT`
- `UPDATE` expressions and predicates
- `DELETE` predicates
- transaction control
- type, nullability, primary-key, text-length, and unique constraints
- SQL three-valued boolean logic and checked numeric arithmetic
- primary-key and unique-column equality point access for SELECT/UPDATE/DELETE,
  with scan fallback for other predicates

## Remaining work

- secondary `CREATE INDEX` execution and catalog metadata
- joins, grouping, ordering, parameters, and subqueries
- binder/planner separation and cost-based plans
- non-unique secondary indexes and richer predicate/range planning
- durable schema migrations and broader SQL types
- statement cancellation and query memory budgets
