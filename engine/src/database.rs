use crate::{
    access::{self, AccessPath},
    codec::{
        catalog_key, decode_row, decode_schema, decode_u64, encode_identity, encode_row,
        encode_schema, encode_u64, row_key, row_prefix, table_id_counter_key, unique_key,
        unique_prefix,
    },
    expression::{evaluate, infer_type, predicate},
    EngineError, OutputColumn, Result, StatementOutput, TableSchema, TransactionOutput, Value,
};
use quantadb_mvcc::{MvccDatabase, MvccOptions, Transaction};
use quantadb_syntax::{parse_sql, Delete, Expr, Insert, Select, SelectItem, Statement, Update};
use std::{collections::HashSet, mem, path::Path, sync::Arc};

#[derive(Clone)]
pub struct DatabaseEngine {
    database: Arc<MvccDatabase>,
}

pub struct SqlSession {
    engine: DatabaseEngine,
    state: SessionState,
}

enum SessionState {
    Idle,
    Active(Transaction),
    Failed,
}

impl DatabaseEngine {
    pub fn open(path: impl AsRef<Path>, options: MvccOptions) -> Result<Self> {
        Ok(Self {
            database: Arc::new(MvccDatabase::open(path, options)?),
        })
    }

    #[must_use]
    pub fn session(&self) -> SqlSession {
        SqlSession {
            engine: self.clone(),
            state: SessionState::Idle,
        }
    }
}

impl SqlSession {
    pub fn execute(&mut self, sql: &str) -> Result<Vec<StatementOutput>> {
        let statements = match parse_sql(sql) {
            Ok(statements) => statements,
            Err(error) => {
                if matches!(self.state, SessionState::Active(_)) {
                    self.state = SessionState::Failed;
                }
                return Err(error.into());
            }
        };
        statements
            .into_iter()
            .map(|statement| self.execute_statement(statement))
            .collect()
    }

    fn execute_statement(&mut self, statement: Statement) -> Result<StatementOutput> {
        match statement {
            Statement::BeginTransaction(_) => self.begin(),
            Statement::Commit(_) => self.commit(),
            Statement::Rollback(_) => self.rollback(),
            statement => self.execute_data_statement(statement),
        }
    }

    fn begin(&mut self) -> Result<StatementOutput> {
        if !matches!(self.state, SessionState::Idle) {
            return Err(EngineError::TransactionAlreadyActive);
        }
        self.state = SessionState::Active(self.engine.database.begin()?);
        Ok(StatementOutput::Transaction(TransactionOutput::Begun))
    }

    fn commit(&mut self) -> Result<StatementOutput> {
        match mem::replace(&mut self.state, SessionState::Idle) {
            SessionState::Active(transaction) => {
                transaction.commit()?;
                Ok(StatementOutput::Transaction(TransactionOutput::Committed))
            }
            SessionState::Failed => {
                self.state = SessionState::Failed;
                Err(EngineError::TransactionAborted)
            }
            SessionState::Idle => Err(EngineError::NoActiveTransaction),
        }
    }

    fn rollback(&mut self) -> Result<StatementOutput> {
        match mem::replace(&mut self.state, SessionState::Idle) {
            SessionState::Active(transaction) => transaction.rollback()?,
            SessionState::Failed => {}
            SessionState::Idle => return Err(EngineError::NoActiveTransaction),
        }
        Ok(StatementOutput::Transaction(TransactionOutput::RolledBack))
    }

    fn execute_data_statement(&mut self, statement: Statement) -> Result<StatementOutput> {
        match mem::replace(&mut self.state, SessionState::Idle) {
            SessionState::Idle => {
                let mut transaction = self.engine.database.begin()?;
                let output = execute_in_transaction(&mut transaction, statement)?;
                transaction.commit()?;
                Ok(output)
            }
            SessionState::Active(mut transaction) => {
                match execute_in_transaction(&mut transaction, statement) {
                    Ok(output) => {
                        self.state = SessionState::Active(transaction);
                        Ok(output)
                    }
                    Err(error) => {
                        drop(transaction);
                        self.state = SessionState::Failed;
                        Err(error)
                    }
                }
            }
            SessionState::Failed => {
                self.state = SessionState::Failed;
                Err(EngineError::TransactionAborted)
            }
        }
    }
}

fn execute_in_transaction(
    transaction: &mut Transaction,
    statement: Statement,
) -> Result<StatementOutput> {
    match statement {
        Statement::CreateTable(create) => {
            let key = catalog_key(&create.name.value)?;
            if transaction.get(&key)?.is_some() {
                return if create.if_not_exists {
                    Ok(command("CREATE TABLE", 0))
                } else {
                    Err(EngineError::TableAlreadyExists(create.name.value))
                };
            }
            let next_id = transaction
                .get(table_id_counter_key())?
                .map_or(Ok(1), |bytes| decode_u64(&bytes))?;
            let following_id = next_id
                .checked_add(1)
                .ok_or_else(|| EngineError::InvalidSchema("table ID space exhausted".to_owned()))?;
            let schema = TableSchema::from_create(next_id, &create)?;
            transaction.put(key, encode_schema(&schema)?)?;
            transaction.put(table_id_counter_key(), encode_u64(following_id))?;
            Ok(command("CREATE TABLE", 0))
        }
        Statement::DropTable(drop) => {
            let key = catalog_key(&drop.name.value)?;
            let Some(encoded) = transaction.get(&key)? else {
                return if drop.if_exists {
                    Ok(command("DROP TABLE", 0))
                } else {
                    Err(EngineError::TableNotFound(drop.name.value))
                };
            };
            let schema = decode_schema(&encoded)?;
            for (row_key, _) in transaction.scan_prefix(&row_prefix(schema.id))? {
                transaction.delete(row_key)?;
            }
            for (constraint_key, _) in transaction.scan_prefix(&unique_prefix(schema.id))? {
                transaction.delete(constraint_key)?;
            }
            transaction.delete(key)?;
            Ok(command("DROP TABLE", 0))
        }
        Statement::Insert(insert) => execute_insert(transaction, insert),
        Statement::Select(select) => execute_select(transaction, select),
        Statement::Update(update) => execute_update(transaction, update),
        Statement::Delete(delete) => execute_delete(transaction, delete),
        other => Err(EngineError::Unsupported(format!(
            "{other:?} execution is not implemented yet"
        ))),
    }
}

fn execute_update(transaction: &mut Transaction, update: Update) -> Result<StatementOutput> {
    let schema = load_schema(transaction, &update.table.value)?;
    let mut assignment_positions = HashSet::new();
    let assignments = update
        .assignments
        .iter()
        .map(|assignment| {
            let position = schema
                .columns
                .iter()
                .position(|column| column.name == assignment.column.value)
                .ok_or_else(|| EngineError::ColumnNotFound(assignment.column.value.clone()))?;
            if !assignment_positions.insert(position) {
                return Err(EngineError::InvalidRow(format!(
                    "column {} is assigned more than once",
                    assignment.column.value
                )));
            }
            Ok((position, &assignment.value))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut rows = read_rows(transaction, &schema, update.selection.as_ref())?;
    let mut affected = 0_u64;

    for position in 0..rows.len() {
        let original = rows[position].1.clone();
        let selected = match &update.selection {
            Some(selection) => predicate(selection, &schema, &original)?,
            None => true,
        };
        if !selected {
            continue;
        }
        let mut updated = original.clone();
        for (column_position, expression) in &assignments {
            updated[*column_position] = coerce(
                evaluate(expression, &schema, &original)?,
                &schema.columns[*column_position].data_type,
            )?;
        }
        schema.validate_row(&updated)?;
        validate_unique_excluding(&schema, &updated, &rows, position)?;

        let new_key = if let Some(primary_key) = schema.primary_key {
            row_key(schema.id, &encode_identity(&updated[primary_key])?)?
        } else {
            rows[position].0.clone()
        };
        let old_key = rows[position].0.clone();
        update_unique_keys(
            transaction,
            &schema,
            &original,
            &updated,
            &old_key,
            &new_key,
        )?;
        if new_key != old_key {
            if transaction.get(&new_key)?.is_some()
                || rows
                    .iter()
                    .enumerate()
                    .any(|(other, (key, _))| other != position && *key == new_key)
            {
                return Err(EngineError::ConstraintViolation(
                    "duplicate primary key".to_owned(),
                ));
            }
            transaction.delete(old_key)?;
        }
        transaction.put(new_key.clone(), encode_row(&updated)?)?;
        rows[position] = (new_key, updated);
        affected += 1;
    }
    Ok(command("UPDATE", affected))
}

fn execute_delete(transaction: &mut Transaction, delete: Delete) -> Result<StatementOutput> {
    let schema = load_schema(transaction, &delete.table.value)?;
    let mut affected = 0_u64;
    for (key, values) in read_rows(transaction, &schema, delete.selection.as_ref())? {
        schema.validate_row(&values)?;
        let selected = match &delete.selection {
            Some(selection) => predicate(selection, &schema, &values)?,
            None => true,
        };
        if selected {
            release_unique_keys(transaction, &schema, &values)?;
            transaction.delete(key)?;
            affected += 1;
        }
    }
    Ok(command("DELETE", affected))
}

fn execute_insert(transaction: &mut Transaction, insert: Insert) -> Result<StatementOutput> {
    let schema_key = catalog_key(&insert.table.value)?;
    let mut schema = load_schema(transaction, &insert.table.value)?;
    let positions = if insert.columns.is_empty() {
        (0..schema.columns.len()).collect::<Vec<_>>()
    } else {
        let mut seen = HashSet::new();
        insert
            .columns
            .iter()
            .map(|identifier| {
                if !seen.insert(identifier.value.clone()) {
                    return Err(EngineError::InvalidRow(format!(
                        "column {} appears more than once",
                        identifier.value
                    )));
                }
                schema
                    .columns
                    .iter()
                    .position(|column| column.name == identifier.value)
                    .ok_or_else(|| EngineError::ColumnNotFound(identifier.value.clone()))
            })
            .collect::<Result<Vec<_>>>()?
    };
    let existing_rows = transaction
        .scan_prefix(&row_prefix(schema.id))?
        .into_iter()
        .map(|(_, bytes)| decode_row(&bytes))
        .collect::<Result<Vec<_>>>()?;
    let mut new_rows = Vec::new();

    for expressions in insert.rows {
        if expressions.len() != positions.len() {
            return Err(EngineError::InvalidRow(format!(
                "INSERT specifies {} columns but supplies {} values",
                positions.len(),
                expressions.len()
            )));
        }
        let mut values = vec![Value::Null; schema.columns.len()];
        for (expression, position) in expressions.iter().zip(&positions) {
            let value = evaluate(expression, &schema, &values)?;
            values[*position] = coerce(value, &schema.columns[*position].data_type)?;
        }
        schema.validate_row(&values)?;
        validate_unique_values(
            &schema,
            &values,
            existing_rows.iter().chain(new_rows.iter()),
        )?;

        let identity = if let Some(primary_key) = schema.primary_key {
            encode_identity(&values[primary_key])?
        } else {
            let identity = schema.next_row_id.to_be_bytes().to_vec();
            schema.next_row_id = schema.next_row_id.checked_add(1).ok_or_else(|| {
                EngineError::ConstraintViolation("hidden row ID space exhausted".to_owned())
            })?;
            identity
        };
        let key = row_key(schema.id, &identity)?;
        if transaction.get(&key)?.is_some() {
            return Err(EngineError::ConstraintViolation(
                "duplicate primary key".to_owned(),
            ));
        }
        acquire_unique_keys(transaction, &schema, &values, &key)?;
        transaction.put(key, encode_row(&values)?)?;
        new_rows.push(values);
    }
    if schema.primary_key.is_none() {
        transaction.put(schema_key, encode_schema(&schema)?)?;
    }
    Ok(command("INSERT", new_rows.len() as u64))
}

fn execute_select(transaction: &Transaction, select: Select) -> Result<StatementOutput> {
    let schema = load_schema(transaction, &select.from.value)?;
    let columns = projection_columns(&schema, &select.projection)?;
    for key in &select.order_by {
        infer_type(&key.expression, &schema)?;
    }
    let mut rows = Vec::new();
    if select.limit == Some(0) {
        return Ok(StatementOutput::Query { columns, rows });
    }

    let mut sortable = Vec::new();
    for (_, values) in read_rows(transaction, &schema, select.selection.as_ref())? {
        schema.validate_row(&values)?;
        if let Some(selection) = &select.selection {
            if !predicate(selection, &schema, &values)? {
                continue;
            }
        }
        let mut projected = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::Wildcard { .. } => projected.extend(values.iter().cloned()),
                SelectItem::Expression { expression, .. } => {
                    projected.push(evaluate(expression, &schema, &values)?);
                }
            }
        }
        if select.order_by.is_empty() {
            rows.push(projected);
            if select.limit.is_some_and(|limit| rows.len() as u64 >= limit) {
                break;
            }
        } else {
            let keys = select
                .order_by
                .iter()
                .map(|key| evaluate(&key.expression, &schema, &values))
                .collect::<Result<Vec<_>>>()?;
            sortable.push((keys, projected));
        }
    }

    if !select.order_by.is_empty() {
        sortable.sort_by(|(left, _), (right, _)| compare_order_keys(left, right, &select.order_by));
        if let Some(limit) = select.limit {
            sortable.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        }
        rows = sortable.into_iter().map(|(_, row)| row).collect();
    }
    Ok(StatementOutput::Query { columns, rows })
}

/// Compare two rows key by key with SQL ordering.
///
/// Nulls sort after every value ascending and before every value
/// descending, matching the usual SQL default. Integers and floats compare
/// numerically; every other comparison is within one type because order
/// keys are type checked before execution.
fn compare_order_keys(
    left: &[Value],
    right: &[Value],
    order_by: &[quantadb_syntax::OrderKey],
) -> std::cmp::Ordering {
    for (position, key) in order_by.iter().enumerate() {
        let ordering = compare_sort_values(&left[position], &right[position]);
        let ordering = if key.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_sort_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Integer(left), Value::Integer(right)) => left.cmp(right),
        (Value::Float(left), Value::Float(right)) => left.total_cmp(right),
        (Value::Integer(left), Value::Float(right)) => (*left as f64).total_cmp(right),
        (Value::Float(left), Value::Integer(right)) => left.total_cmp(&(*right as f64)),
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        (left, right) => type_rank(left).cmp(&type_rank(right)),
    }
}

/// A last-resort total order between values of different types.
///
/// Type checking keeps mixed-type keys out of real queries; this exists so
/// the comparator stays total no matter what.
const fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Boolean(_) => 1,
        Value::Integer(_) | Value::Float(_) => 2,
        Value::Text(_) => 3,
    }
}

fn load_schema(transaction: &Transaction, name: &str) -> Result<TableSchema> {
    transaction
        .get(&catalog_key(name)?)?
        .ok_or_else(|| EngineError::TableNotFound(name.to_owned()))
        .and_then(|bytes| decode_schema(&bytes))
}

fn read_rows(
    transaction: &Transaction,
    schema: &TableSchema,
    selection: Option<&Expr>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    match access::plan(transaction, schema, selection)? {
        AccessPath::Scan => transaction
            .scan_prefix(&row_prefix(schema.id))?
            .into_iter()
            .map(|(key, bytes)| Ok((key, decode_row(&bytes)?)))
            .collect(),
        AccessPath::Point(None) => Ok(Vec::new()),
        AccessPath::Point(Some(key)) => transaction
            .get(&key)?
            .map(|bytes| decode_row(&bytes).map(|row| vec![(key, row)]))
            .unwrap_or_else(|| Ok(Vec::new())),
    }
}

fn validate_unique_values<'a>(
    schema: &TableSchema,
    candidate: &[Value],
    rows: impl Iterator<Item = &'a Vec<Value>>,
) -> Result<()> {
    for row in rows {
        for (position, column) in schema.columns.iter().enumerate() {
            if (column.unique || column.primary_key)
                && !candidate[position].is_null()
                && candidate[position] == row[position]
            {
                return Err(EngineError::ConstraintViolation(format!(
                    "duplicate value for unique column {}",
                    column.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_unique_excluding(
    schema: &TableSchema,
    candidate: &[Value],
    rows: &[(Vec<u8>, Vec<Value>)],
    excluded: usize,
) -> Result<()> {
    validate_unique_values(
        schema,
        candidate,
        rows.iter()
            .enumerate()
            .filter(|(position, _)| *position != excluded)
            .map(|(_, (_, values))| values),
    )
}

fn acquire_unique_keys(
    transaction: &mut Transaction,
    schema: &TableSchema,
    values: &[Value],
    row_key: &[u8],
) -> Result<()> {
    for (position, column) in schema.columns.iter().enumerate() {
        if column.unique && !column.primary_key && !values[position].is_null() {
            let key = unique_key(schema.id, position, &values[position])?;
            if transaction.get(&key)?.is_some() {
                return Err(EngineError::ConstraintViolation(format!(
                    "duplicate value for unique column {}",
                    column.name
                )));
            }
            transaction.put(key, row_key.to_vec())?;
        }
    }
    Ok(())
}

fn release_unique_keys(
    transaction: &mut Transaction,
    schema: &TableSchema,
    values: &[Value],
) -> Result<()> {
    for (position, column) in schema.columns.iter().enumerate() {
        if column.unique && !column.primary_key && !values[position].is_null() {
            transaction.delete(unique_key(schema.id, position, &values[position])?)?;
        }
    }
    Ok(())
}

fn update_unique_keys(
    transaction: &mut Transaction,
    schema: &TableSchema,
    old: &[Value],
    new: &[Value],
    old_row_key: &[u8],
    new_row_key: &[u8],
) -> Result<()> {
    for (position, column) in schema.columns.iter().enumerate() {
        if !column.unique || column.primary_key {
            continue;
        }
        let value_changed = old[position] != new[position];
        let row_key_changed = old_row_key != new_row_key;
        if !value_changed && !row_key_changed {
            continue;
        }
        if !new[position].is_null() {
            let key = unique_key(schema.id, position, &new[position])?;
            let owner = transaction.get(&key)?;
            if value_changed && owner.is_some() {
                return Err(EngineError::ConstraintViolation(format!(
                    "duplicate value for unique column {}",
                    column.name
                )));
            }
            transaction.put(key, new_row_key.to_vec())?;
        }
        if value_changed && !old[position].is_null() {
            transaction.delete(unique_key(schema.id, position, &old[position])?)?;
        }
    }
    Ok(())
}

fn projection_columns(
    schema: &TableSchema,
    projection: &[SelectItem],
) -> Result<Vec<OutputColumn>> {
    let mut columns = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard { .. } => {
                columns.extend(schema.columns.iter().map(|column| OutputColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                }));
            }
            SelectItem::Expression {
                expression, alias, ..
            } => {
                let (data_type, nullable) = infer_type(expression, schema)?;
                columns.push(OutputColumn {
                    name: alias.as_ref().map_or_else(
                        || expression_name(expression),
                        |identifier| identifier.value.clone(),
                    ),
                    data_type,
                    nullable,
                });
            }
        }
    }
    Ok(columns)
}

fn expression_name(expression: &Expr) -> String {
    match expression {
        Expr::Identifier(identifier) => identifier.value.clone(),
        _ => "?column?".to_owned(),
    }
}

fn coerce(value: Value, data_type: &crate::LogicalType) -> Result<Value> {
    match (value, data_type) {
        (Value::Integer(value), crate::LogicalType::Float64) => Ok(Value::Float(value as f64)),
        (value, _) => Ok(value),
    }
}

fn command(tag: &str, affected_rows: u64) -> StatementOutput {
    StatementOutput::Command {
        tag: tag.to_owned(),
        affected_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn query_rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
        match session.execute(sql).expect("query").pop().expect("output") {
            StatementOutput::Query { rows, .. } => rows,
            other => panic!("expected a query result, got {other:?}"),
        }
    }

    #[test]
    fn order_by_sorts_keys_directions_and_nulls() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let mut session = engine.session();
        session
            .execute(
                "CREATE TABLE ranked (id BIGINT PRIMARY KEY, score DOUBLE, team TEXT NOT NULL)",
            )
            .expect("create");
        session
            .execute(
                "INSERT INTO ranked (id, score, team) VALUES \
                 (1, 7.5, 'blue'), (2, NULL, 'red'), (3, 9.0, 'blue'), \
                 (4, 7.5, 'red'), (5, 1.0, 'green')",
            )
            .expect("insert");

        let rows = query_rows(
            &mut session,
            "SELECT id FROM ranked ORDER BY score DESC, id",
        );
        assert_eq!(
            rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
            vec![
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(1),
                Value::Integer(4),
                Value::Integer(5),
            ],
            "descending puts nulls first, ties break on the second key"
        );

        let rows = query_rows(&mut session, "SELECT id FROM ranked ORDER BY score LIMIT 2");
        assert_eq!(
            rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(5), Value::Integer(1)],
            "ascending puts nulls last and LIMIT applies after the sort"
        );

        let rows = query_rows(
            &mut session,
            "SELECT id, score FROM ranked WHERE team = 'blue' ORDER BY 0 - id",
        );
        assert_eq!(
            rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(3), Value::Integer(1)],
            "order keys are full expressions and need not be projected"
        );

        assert!(
            session
                .execute("SELECT id FROM ranked ORDER BY missing")
                .is_err(),
            "unknown order keys fail before any row is read"
        );
    }

    #[test]
    fn catalog_ddl_is_transactional_and_survives_restart() {
        let directory = tempdir().expect("tempdir");
        {
            let engine =
                DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
            let mut session = engine.session();
            session
                .execute("BEGIN; CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT); COMMIT")
                .expect("create table");
        }
        {
            let engine =
                DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("reopen");
            let mut session = engine.session();
            assert!(matches!(
                session.execute("CREATE TABLE users (id BIGINT PRIMARY KEY)"),
                Err(EngineError::TableAlreadyExists(name)) if name == "users"
            ));
            session.execute("DROP TABLE users").expect("drop table");
        }
    }

    #[test]
    fn failed_explicit_transaction_requires_rollback() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let mut session = engine.session();
        session.execute("BEGIN").expect("begin");
        assert!(session.execute("SELECT * FROM missing").is_err());
        assert!(matches!(
            session.execute("COMMIT"),
            Err(EngineError::TransactionAborted)
        ));
        session.execute("ROLLBACK").expect("rollback");
    }

    #[test]
    fn inserts_and_selects_typed_rows_with_constraints() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let mut session = engine.session();
        session
            .execute(
                "CREATE TABLE users (
                    id BIGINT PRIMARY KEY,
                    name VARCHAR(20) NOT NULL,
                    score DOUBLE,
                    active BOOL
                )",
            )
            .expect("create");
        assert_eq!(
            session
                .execute(
                    "INSERT INTO users (id, name, score, active) VALUES
                     (1, 'Ada', 9.5, true), (2, 'Grace', 8, false)"
                )
                .expect("insert"),
            vec![command("INSERT", 2)]
        );
        let output = session
            .execute(
                "SELECT id, name, score + 1 AS adjusted
                 FROM users WHERE active = true LIMIT 10",
            )
            .expect("select");
        let StatementOutput::Query { columns, rows } = &output[0] else {
            panic!("expected query");
        };
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "adjusted"]
        );
        assert_eq!(
            rows,
            &vec![vec![
                Value::Integer(1),
                Value::Text("Ada".to_owned()),
                Value::Float(10.5),
            ]]
        );
        assert!(matches!(
            session.execute("INSERT INTO users VALUES (1, 'Duplicate', 1, true)"),
            Err(EngineError::ConstraintViolation(_))
        ));

        assert_eq!(
            session
                .execute("UPDATE users SET score = score + 2, active = true WHERE id = 2")
                .expect("update"),
            vec![command("UPDATE", 1)]
        );
        assert_eq!(
            session
                .execute("DELETE FROM users WHERE score >= 10")
                .expect("delete"),
            vec![command("DELETE", 1)]
        );
        let selected = session
            .execute("SELECT id, score FROM users WHERE active = true")
            .expect("select after mutations");
        let StatementOutput::Query { rows, .. } = &selected[0] else {
            panic!("expected query");
        };
        assert_eq!(rows, &vec![vec![Value::Integer(1), Value::Float(9.5)]]);
    }

    #[test]
    fn explicit_rollback_discards_rows_and_schema_changes() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let mut session = engine.session();
        session.execute("BEGIN").expect("begin");
        session
            .execute("CREATE TABLE temporary (id BIGINT PRIMARY KEY)")
            .expect("create");
        session
            .execute("INSERT INTO temporary VALUES (1)")
            .expect("insert");
        session.execute("ROLLBACK").expect("rollback");
        assert!(matches!(
            session.execute("SELECT * FROM temporary"),
            Err(EngineError::TableNotFound(_))
        ));
    }

    #[test]
    fn concurrent_unique_values_conflict_at_mvcc_commit() {
        let directory = tempdir().expect("tempdir");
        let engine =
            DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
        let mut setup = engine.session();
        setup
            .execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
            .expect("create");

        let mut first = engine.session();
        let mut second = engine.session();
        first.execute("BEGIN").expect("first begin");
        second.execute("BEGIN").expect("second begin");
        first
            .execute("INSERT INTO users VALUES (1, 'same@example.com')")
            .expect("first insert");
        second
            .execute("INSERT INTO users VALUES (2, 'same@example.com')")
            .expect("second insert in snapshot");
        first.execute("COMMIT").expect("first commit");
        assert!(matches!(
            second.execute("COMMIT"),
            Err(EngineError::Transaction(
                quantadb_mvcc::TransactionError::WriteConflict { .. }
            ))
        ));
    }

    #[test]
    fn primary_and_unique_point_paths_track_key_changes_and_restart() {
        let directory = tempdir().expect("tempdir");
        {
            let engine =
                DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("engine");
            let mut session = engine.session();
            session
                .execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT UNIQUE)")
                .expect("create");
            session
                .execute("INSERT INTO users VALUES (1, 'old@example.com')")
                .expect("insert");
            session
                .execute("UPDATE users SET id = 9 WHERE email = 'old@example.com'")
                .expect("change primary key through unique path");
            session
                .execute("UPDATE users SET email = 'new@example.com' WHERE id = 9")
                .expect("change unique value through primary path");

            assert_query_rows(
                &mut session,
                "SELECT id FROM users WHERE email = 'new@example.com'",
                vec![vec![Value::Integer(9)]],
            );
            assert_query_rows(
                &mut session,
                "SELECT id FROM users WHERE email = 'old@example.com'",
                Vec::new(),
            );
        }
        {
            let engine =
                DatabaseEngine::open(directory.path(), MvccOptions::default()).expect("reopen");
            let mut session = engine.session();
            assert_query_rows(
                &mut session,
                "SELECT email FROM users WHERE id = 9",
                vec![vec![Value::Text("new@example.com".to_owned())]],
            );
            session
                .execute("DELETE FROM users WHERE email = 'new@example.com'")
                .expect("delete through unique path");
            assert_query_rows(&mut session, "SELECT * FROM users WHERE id = 9", Vec::new());
        }
    }

    fn assert_query_rows(session: &mut SqlSession, sql: &str, expected: Vec<Vec<Value>>) {
        let output = session.execute(sql).expect("query");
        let StatementOutput::Query { rows, .. } = &output[0] else {
            panic!("expected query output");
        };
        assert_eq!(rows, &expected);
    }
}
