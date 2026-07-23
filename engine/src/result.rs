use crate::{LogicalType, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum StatementOutput {
    Transaction(TransactionOutput),
    Command {
        tag: String,
        affected_rows: u64,
    },
    Query {
        columns: Vec<OutputColumn>,
        rows: Vec<Vec<Value>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionOutput {
    Begun,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumn {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
}
