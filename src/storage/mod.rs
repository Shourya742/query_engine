use std::sync::Arc;

use arrow::{error::ArrowError, record_batch::RecordBatch};

mod csv;
pub use csv::*;

use crate::catalog::RootCatalog;

#[derive(Clone)]
pub enum StorageImpl {
    CsvStorage(Arc<CsvStorage>)
}

pub trait Storage: Sync + Send + 'static {
    type TableType: Table;

    fn create_table(&self, id: String, filepath: String) -> Result<(), StorageError>;

    fn get_table(&self, id: String) -> Result<Self::TableType, StorageError>;

    fn get_catalog(&self) -> RootCatalog;
}

pub trait Table: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    fn read(&self) -> Result<Self::TransactionType, StorageError>;
}

pub trait Transaction: Sync + Send + 'static {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, StorageError>;
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("io error")]
    IoError(#[from] std::io::Error),

    #[error("table not found: {0}")]
    TableNotFound(String),
}
