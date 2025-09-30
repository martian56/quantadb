use crate::storage::{Schema, Row};
use crate::error::{QuantaError, Result};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub schema: Schema,
    pub rows: Arc<DashMap<u64, Row>>,
    pub next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl Table {
    pub fn new(name: String, schema: Schema) -> Self {
        Self {
            name,
            schema,
            rows: Arc::new(DashMap::new()),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    pub fn insert_row(&self, row: Row) -> Result<u64> {
        // Validate the row against the schema
        if !self.schema.validate_row(&row) {
            return Err(QuantaError::QueryError(
                format!("Row does not match schema for table '{}'", self.name)
            ));
        }

        // Generate a unique ID for this row
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        // Insert the row
        self.rows.insert(id, row);
        
        Ok(id)
    }

    pub fn get_all_rows(&self) -> Vec<(u64, Row)> {
        self.rows.iter().map(|entry| (*entry.key(), entry.value().clone())).collect()
    }

    pub fn get_row(&self, id: u64) -> Option<Row> {
        self.rows.get(&id).map(|entry| entry.value().clone())
    }

    pub fn delete_row(&self, id: u64) -> bool {
        self.rows.remove(&id).is_some()
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }
}
