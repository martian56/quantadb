use crate::storage::{Column, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    Delete(DeleteStmt),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStmt {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStmt {
    pub table_name: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStmt {
    pub table_name: String,
    pub columns: SelectColumns,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectColumns {
    All,
    Specific(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    Comparison {
        left: Expression,
        operator: ComparisonOp,
        right: Expression,
    },
    And {
        left: Box<Condition>,
        right: Box<Condition>,
    },
    Or {
        left: Box<Condition>,
        right: Box<Condition>,
    },
    Not {
        condition: Box<Condition>,
    },
    Parenthesized {
        condition: Box<Condition>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Column(String),
    Literal(Value),
}

impl Expression {
    pub fn column(name: String) -> Self {
        Self::Column(name)
    }

    pub fn literal(value: Value) -> Self {
        Self::Literal(value)
    }
}
