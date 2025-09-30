use crate::storage::{Table, Schema, Row};
use crate::error::{QuantaError, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub schema: Schema,
    pub next_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedTable {
    pub metadata: TableMetadata,
    pub rows: Vec<(u64, Row)>,
}

#[derive(Debug)]
pub struct FileStorage {
    data_dir: PathBuf,
}

impl FileStorage {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    pub fn ensure_data_dir(&self) -> Result<()> {
        if !self.data_dir.exists() {
            fs::create_dir_all(&self.data_dir)
                .map_err(|e| QuantaError::StorageError(format!("Failed to create data directory: {}", e)))?;
        }
        Ok(())
    }

    pub fn save_table(&self, table: &Table) -> Result<()> {
        self.ensure_data_dir()?;

        let metadata = TableMetadata {
            name: table.name.clone(),
            schema: table.schema.clone(),
            next_id: table.next_id.load(std::sync::atomic::Ordering::SeqCst),
        };

        let rows = table.get_all_rows();
        let persisted_table = PersistedTable {
            metadata,
            rows,
        };

        let table_file = self.data_dir.join(format!("{}.json", table.name));
        let json_data = serde_json::to_string_pretty(&persisted_table)
            .map_err(|e| QuantaError::StorageError(format!("Failed to serialize table: {}", e)))?;

        fs::write(&table_file, json_data)
            .map_err(|e| QuantaError::StorageError(format!("Failed to write table file: {}", e)))?;

        Ok(())
    }

    pub fn load_table(&self, table_name: &str) -> Result<Option<Table>> {
        let table_file = self.data_dir.join(format!("{}.json", table_name));
        
        if !table_file.exists() {
            return Ok(None);
        }

        let json_data = fs::read_to_string(&table_file)
            .map_err(|e| QuantaError::StorageError(format!("Failed to read table file: {}", e)))?;

        let persisted_table: PersistedTable = serde_json::from_str(&json_data)
            .map_err(|e| QuantaError::StorageError(format!("Failed to deserialize table: {}", e)))?;

        let table = Table::new(
            persisted_table.metadata.name,
            persisted_table.metadata.schema,
        );

        // Restore the next_id
        table.next_id.store(
            persisted_table.metadata.next_id,
            std::sync::atomic::Ordering::SeqCst,
        );

        // Restore the rows
        for (id, row) in persisted_table.rows {
            table.rows.insert(id, row);
        }

        Ok(Some(table))
    }

    pub fn delete_table(&self, table_name: &str) -> Result<()> {
        let table_file = self.data_dir.join(format!("{}.json", table_name));
        
        if table_file.exists() {
            fs::remove_file(&table_file)
                .map_err(|e| QuantaError::StorageError(format!("Failed to delete table file: {}", e)))?;
        }

        Ok(())
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        self.ensure_data_dir()?;

        let mut tables = Vec::new();
        
        for entry in WalkDir::new(&self.data_dir)
            .max_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                if let Some(file_name) = entry.file_name().to_str() {
                    if file_name.ends_with(".json") {
                        let table_name = file_name.trim_end_matches(".json");
                        tables.push(table_name.to_string());
                    }
                }
            }
        }

        Ok(tables)
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        let table_file = self.data_dir.join(format!("{}.json", table_name));
        table_file.exists()
    }

    pub fn get_data_dir(&self) -> &Path {
        &self.data_dir
    }
}
