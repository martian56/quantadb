use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Text,
    Bool,
    Float,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int => write!(f, "INT"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Bool => write!(f, "BOOL"),
            DataType::Float => write!(f, "FLOAT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Float(f64),
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::Text(s) => write!(f, "\"{}\"", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl Value {
    pub fn get_type(&self) -> DataType {
        match self {
            Value::Int(_) => DataType::Int,
            Value::Text(_) => DataType::Text,
            Value::Bool(_) => DataType::Bool,
            Value::Float(_) => DataType::Float,
            Value::Null => DataType::Int, // Default for null
        }
    }

    pub fn matches_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (Value::Int(_), DataType::Int) => true,
            (Value::Text(_), DataType::Text) => true,
            (Value::Bool(_), DataType::Bool) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Null, _) => true, // Null matches any type
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn get_value_by_name(&self, name: &str, columns: &[Column]) -> Option<&Value> {
        if let Some(index) = columns.iter().position(|col| col.name == name) {
            self.get_value(index)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    pub fn validate_row(&self, row: &Row) -> bool {
        if row.values.len() != self.columns.len() {
            return false;
        }

        for (value, column) in row.values.iter().zip(self.columns.iter()) {
            if !value.matches_type(&column.data_type) {
                return false;
            }
        }

        true
    }
}
