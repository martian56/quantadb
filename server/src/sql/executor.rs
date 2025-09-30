use crate::sql::ast::*;
use crate::storage::{StorageEngine, Row, Value};
use crate::error::{QuantaError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub success: bool,
    pub message: String,
    pub data: Option<Vec<Row>>,
    pub affected_rows: Option<usize>,
}

pub struct QueryExecutor {
    storage: StorageEngine,
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            storage: StorageEngine::new(),
        }
    }

    pub fn new_with_persistence(data_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            storage: StorageEngine::new_with_persistence(data_dir)?,
        })
    }

    pub fn execute(&self, statement: Statement) -> Result<QueryResult> {
        match statement {
            Statement::CreateTable(stmt) => self.execute_create_table(stmt),
            Statement::DropTable(stmt) => self.execute_drop_table(stmt),
            Statement::Insert(stmt) => self.execute_insert(stmt),
            Statement::Select(stmt) => self.execute_select(stmt),
            Statement::Delete(stmt) => self.execute_delete(stmt),
        }
    }

    fn execute_create_table(&self, stmt: CreateTableStmt) -> Result<QueryResult> {
        self.storage.create_table(stmt.table_name.clone(), stmt.columns)?;
        Ok(QueryResult {
            success: true,
            message: format!("Table '{}' created successfully", stmt.table_name),
            data: None,
            affected_rows: None,
        })
    }

    fn execute_drop_table(&self, stmt: DropTableStmt) -> Result<QueryResult> {
        self.storage.drop_table(&stmt.table_name)?;
        Ok(QueryResult {
            success: true,
            message: format!("Table '{}' dropped successfully", stmt.table_name),
            data: None,
            affected_rows: None,
        })
    }

    fn execute_insert(&self, stmt: InsertStmt) -> Result<QueryResult> {
        let row_id = self.storage.insert_into_table(&stmt.table_name, stmt.values)?;
        Ok(QueryResult {
            success: true,
            message: format!("Row inserted with ID: {}", row_id),
            data: None,
            affected_rows: Some(1),
        })
    }

    fn execute_select(&self, stmt: SelectStmt) -> Result<QueryResult> {
        let table = self.storage.get_table(&stmt.table_name)?;
        let all_rows = table.get_all_rows();
        
        let filtered_rows = if let Some(where_clause) = stmt.where_clause {
            self.filter_rows(all_rows, &where_clause.condition, table.get_schema())?
        } else {
            all_rows
        };

        let selected_rows = match stmt.columns {
            SelectColumns::All => {
                filtered_rows.into_iter().map(|(_, row)| row).collect()
            }
            SelectColumns::Specific(column_names) => {
                self.select_columns(filtered_rows, &column_names, table.get_schema())?
            }
        };

        Ok(QueryResult {
            success: true,
            message: format!("Query executed successfully, {} rows returned", selected_rows.len()),
            data: Some(selected_rows),
            affected_rows: None,
        })
    }

    fn execute_delete(&self, stmt: DeleteStmt) -> Result<QueryResult> {
        let table = self.storage.get_table(&stmt.table_name)?;
        let all_rows = table.get_all_rows();
        
        let rows_to_delete = if let Some(where_clause) = stmt.where_clause {
            self.filter_rows(all_rows, &where_clause.condition, table.get_schema())?
        } else {
            all_rows
        };

        let mut deleted_count = 0;
        for (id, _) in rows_to_delete {
            if table.delete_row(id) {
                deleted_count += 1;
            }
        }

        Ok(QueryResult {
            success: true,
            message: format!("{} rows deleted", deleted_count),
            data: None,
            affected_rows: Some(deleted_count),
        })
    }

    fn filter_rows(&self, rows: Vec<(u64, Row)>, condition: &Condition, schema: &crate::storage::Schema) -> Result<Vec<(u64, Row)>> {
        let mut result = Vec::new();
        for (id, row) in rows {
            if self.evaluate_condition(condition, &row, schema)? {
                result.push((id, row));
            }
        }
        Ok(result)
    }

    fn evaluate_condition(&self, condition: &Condition, row: &Row, schema: &crate::storage::Schema) -> Result<bool> {
        match condition {
            Condition::Comparison { left, operator, right } => {
                let left_val = self.evaluate_expression(left, row, schema)?;
                let right_val = self.evaluate_expression(right, row, schema)?;
                Ok(self.compare_values(left_val, operator, right_val)?)
            }
            Condition::And { left, right } => {
                Ok(self.evaluate_condition(left, row, schema)? && self.evaluate_condition(right, row, schema)?)
            }
            Condition::Or { left, right } => {
                Ok(self.evaluate_condition(left, row, schema)? || self.evaluate_condition(right, row, schema)?)
            }
            Condition::Not { condition } => {
                Ok(!self.evaluate_condition(condition, row, schema)?)
            }
            Condition::Parenthesized { condition } => {
                self.evaluate_condition(condition, row, schema)
            }
        }
    }

    fn evaluate_expression(&self, expr: &Expression, row: &Row, schema: &crate::storage::Schema) -> Result<Value> {
        match expr {
            Expression::Column(name) => {
                if let Some(value) = row.get_value_by_name(name, &schema.columns) {
                    Ok(value.clone())
                } else {
                    Err(QuantaError::ColumnNotFound(name.clone()))
                }
            }
            Expression::Literal(value) => Ok(value.clone()),
        }
    }

    fn compare_values(&self, left: Value, op: &ComparisonOp, right: Value) -> Result<bool> {
        match (left, right) {
            (Value::Int(l), Value::Int(r)) => Ok(match op {
                ComparisonOp::Equal => l == r,
                ComparisonOp::NotEqual => l != r,
                ComparisonOp::LessThan => l < r,
                ComparisonOp::GreaterThan => l > r,
                ComparisonOp::LessThanOrEqual => l <= r,
                ComparisonOp::GreaterThanOrEqual => l >= r,
            }),
            (Value::Text(l), Value::Text(r)) => Ok(match op {
                ComparisonOp::Equal => l == r,
                ComparisonOp::NotEqual => l != r,
                ComparisonOp::LessThan => l < r,
                ComparisonOp::GreaterThan => l > r,
                ComparisonOp::LessThanOrEqual => l <= r,
                ComparisonOp::GreaterThanOrEqual => l >= r,
            }),
            (Value::Bool(l), Value::Bool(r)) => Ok(match op {
                ComparisonOp::Equal => l == r,
                ComparisonOp::NotEqual => l != r,
                _ => return Err(QuantaError::QueryError("Boolean comparison only supports = and !=".to_string())),
            }),
            (Value::Float(l), Value::Float(r)) => Ok(match op {
                ComparisonOp::Equal => (l - r).abs() < f64::EPSILON,
                ComparisonOp::NotEqual => (l - r).abs() >= f64::EPSILON,
                ComparisonOp::LessThan => l < r,
                ComparisonOp::GreaterThan => l > r,
                ComparisonOp::LessThanOrEqual => l <= r,
                ComparisonOp::GreaterThanOrEqual => l >= r,
            }),
            _ => Err(QuantaError::QueryError("Type mismatch in comparison".to_string())),
        }
    }

    fn select_columns(&self, rows: Vec<(u64, Row)>, column_names: &[String], schema: &crate::storage::Schema) -> Result<Vec<Row>> {
        let mut result = Vec::new();
        
        for (_, row) in rows {
            let mut new_values = Vec::new();
            for col_name in column_names {
                if let Some(value) = row.get_value_by_name(col_name, &schema.columns) {
                    new_values.push(value.clone());
                } else {
                    return Err(QuantaError::ColumnNotFound(col_name.clone()));
                }
            }
            result.push(Row::new(new_values));
        }
        
        Ok(result)
    }

    pub fn get_storage(&self) -> &StorageEngine {
        &self.storage
    }
}
