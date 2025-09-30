use crate::storage::{Table, Schema, Column, Row, Value, FileStorage};
use crate::error::{QuantaError, Result};
use dashmap::DashMap;
use std::sync::Arc;
use std::path::PathBuf;
use tracing::{info, warn, error};

#[derive(Debug)]
pub struct StorageEngine {
    tables: Arc<DashMap<String, Table>>,
    file_storage: Option<FileStorage>,
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(DashMap::new()),
            file_storage: None,
        }
    }

    pub fn new_with_persistence(data_dir: PathBuf) -> Result<Self> {
        let file_storage = FileStorage::new(data_dir);
        file_storage.ensure_data_dir()?;
        
        let mut engine = Self {
            tables: Arc::new(DashMap::new()),
            file_storage: Some(file_storage),
        };
        
        engine.load_all_tables()?;
        Ok(engine)
    }

    fn load_all_tables(&mut self) -> Result<()> {
        if let Some(ref file_storage) = self.file_storage {
            info!("Loading tables from disk...");
            
            let table_names = file_storage.list_tables()?;
            info!("Found {} tables to load", table_names.len());
            
            for table_name in table_names {
                match file_storage.load_table(&table_name) {
                    Ok(Some(table)) => {
                        info!("Loaded table: {}", table_name);
                        self.tables.insert(table_name, table);
                    }
                    Ok(None) => {
                        warn!("Table file exists but couldn't be loaded: {}", table_name);
                    }
                    Err(e) => {
                        error!("Failed to load table {}: {}", table_name, e);
                        // Continue loading other tables
                    }
                }
            }
            
            info!("Finished loading tables. Total tables in memory: {}", self.tables.len());
        }
        
        Ok(())
    }

    fn save_table_to_disk(&self, table_name: &str) -> Result<()> {
        if let Some(ref file_storage) = self.file_storage {
            if let Some(table) = self.tables.get(table_name) {
                file_storage.save_table(&table)?;
            }
        }
        Ok(())
    }

    pub fn create_table(&self, name: String, columns: Vec<Column>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(QuantaError::QueryError(
                format!("Table '{}' already exists", name)
            ));
        }

        let schema = Schema::new(columns);
        let table = Table::new(name.clone(), schema);
        self.tables.insert(name.clone(), table);
        
        // Save to disk if persistence is enabled
        self.save_table_to_disk(&name)?;
        
        Ok(())
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(QuantaError::TableNotFound(name.to_string()));
        }
        
        // Delete from disk if persistence is enabled
        if let Some(ref file_storage) = self.file_storage {
            file_storage.delete_table(name)?;
        }
        
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<Arc<Table>> {
        if let Some(table) = self.tables.get(name) {
            Ok(Arc::new(table.value().clone()))
        } else {
            Err(QuantaError::TableNotFound(name.to_string()))
        }
    }

    pub fn insert_into_table(&self, table_name: &str, values: Vec<Value>) -> Result<u64> {
        let table = self.get_table(table_name)?;
        let row = Row::new(values);
        let id = table.insert_row(row)?;
        
        // Save to disk if persistence is enabled
        self.save_table_to_disk(table_name)?;
        
        Ok(id)
    }

    pub fn select_from_table(&self, table_name: &str) -> Result<Vec<(u64, Row)>> {
        let table = self.get_table(table_name)?;
        Ok(table.get_all_rows())
    }

    pub fn delete_from_table(&self, table_name: &str, id: u64) -> Result<bool> {
        let table = self.get_table(table_name)?;
        let deleted = table.delete_row(id);
        
        if deleted {
            // Save to disk if persistence is enabled
            self.save_table_to_disk(table_name)?;
        }
        
        Ok(deleted)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
