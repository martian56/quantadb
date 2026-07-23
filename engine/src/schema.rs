use crate::{EngineError, Result, Value};
use quantadb_syntax::{ColumnDef, CreateTable, DataType};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

const SCHEMA_FORMAT_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalType {
    Unknown,
    Boolean,
    Int64,
    Float64,
    Text { max_length: Option<u32> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub format_version: u16,
    pub id: u64,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Option<usize>,
    pub next_row_id: u64,
}

impl TableSchema {
    pub fn from_create(id: u64, create: &CreateTable) -> Result<Self> {
        let columns = create
            .columns
            .iter()
            .map(ColumnSchema::from_definition)
            .collect::<Vec<_>>();
        let primary_keys = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .map(|(position, _)| position)
            .collect::<Vec<_>>();
        if primary_keys.len() > 1 {
            return Err(EngineError::InvalidSchema(
                "only one PRIMARY KEY is currently supported".to_owned(),
            ));
        }
        let mut names = HashSet::new();
        if columns
            .iter()
            .any(|column| !names.insert(column.name.clone()))
        {
            return Err(EngineError::InvalidSchema(
                "column names must be unique".to_owned(),
            ));
        }
        Ok(Self {
            format_version: SCHEMA_FORMAT_VERSION,
            id,
            name: create.name.value.clone(),
            columns,
            primary_key: primary_keys.first().copied(),
            next_row_id: 1,
        })
    }

    pub fn validate_row(&self, values: &[Value]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(EngineError::InvalidRow(format!(
                "table {} expects {} values, got {}",
                self.name,
                self.columns.len(),
                values.len()
            )));
        }
        for (column, value) in self.columns.iter().zip(values) {
            column.validate(value)?;
        }
        Ok(())
    }
}

impl ColumnSchema {
    fn from_definition(column: &ColumnDef) -> Self {
        Self {
            name: column.name.value.clone(),
            data_type: match column.data_type {
                DataType::Boolean => LogicalType::Boolean,
                DataType::Int64 => LogicalType::Int64,
                DataType::Float64 => LogicalType::Float64,
                DataType::Text { max_length } => LogicalType::Text { max_length },
            },
            nullable: column.nullable,
            primary_key: column.primary_key,
            unique: column.unique,
        }
    }

    pub fn validate(&self, value: &Value) -> Result<()> {
        if value.is_null() {
            return if self.nullable {
                Ok(())
            } else {
                Err(EngineError::InvalidRow(format!(
                    "column {} cannot be NULL",
                    self.name
                )))
            };
        }
        let valid = matches!(
            (&self.data_type, value),
            (LogicalType::Unknown, _)
                | (LogicalType::Boolean, Value::Boolean(_))
                | (LogicalType::Int64, Value::Integer(_))
                | (LogicalType::Float64, Value::Float(_))
                | (LogicalType::Text { .. }, Value::Text(_))
        );
        if !valid {
            return Err(EngineError::InvalidRow(format!(
                "value has the wrong type for column {}",
                self.name
            )));
        }
        if let (
            LogicalType::Text {
                max_length: Some(maximum),
            },
            Value::Text(value),
        ) = (&self.data_type, value)
        {
            if value.chars().count() > *maximum as usize {
                return Err(EngineError::InvalidRow(format!(
                    "value for column {} exceeds {} characters",
                    self.name, maximum
                )));
            }
        }
        if matches!(value, Value::Float(number) if !number.is_finite()) {
            return Err(EngineError::InvalidRow(format!(
                "column {} cannot contain a non-finite float",
                self.name
            )));
        }
        Ok(())
    }
}
