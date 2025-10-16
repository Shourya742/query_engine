use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    sync::{Arc, Mutex},
};

use arrow::{
    csv::{reader, Reader, ReaderBuilder},
    datatypes::{Schema, SchemaRef},
};

use crate::{
    catalog::{ColumnCatalog, ColumnDesc, RootCatalog, TableCatalog, TableId},
    storage::{Storage, StorageError, Table, Transaction},
};

#[derive(Default)]
pub struct CsvStorage {
    catalog: Mutex<RootCatalog>,
    tables: Mutex<HashMap<TableId, CsvTable>>,
}

impl Storage for CsvStorage {
    type TableType = CsvTable;

    fn create_table(&self, id: String, filepath: String) -> Result<(), StorageError> {
        let table = CsvTable::new(id.clone(), filepath, CsvConfig::default())?;
        self.catalog
            .lock()
            .unwrap()
            .tables
            .insert(id.clone(), table.catalog.clone());
        self.tables.lock().unwrap().insert(id, table);
        Ok(())
    }

    fn get_table(&self, id: String) -> Result<Self::TableType, StorageError> {
        self.tables
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(StorageError::TableNotFound(id))
    }

    fn get_catalog(&self) -> RootCatalog {
        self.catalog.lock().unwrap().clone()
    }
}

#[derive(Clone)]
pub struct CsvConfig {
    has_header: bool,
    delimiter: u8,
    infer_schema_max_read_records: Option<usize>,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    datetime_format: Option<String>,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            infer_schema_max_read_records: Some(10),
            batch_size: 1024,
            projection: None,
            datetime_format: None,
        }
    }
}

#[derive(Clone)]
pub struct CsvTable {
    id: TableId,
    arrow_schema: SchemaRef,
    arrow_csv_cfg: CsvConfig,
    filepath: String,
    catalog: TableCatalog,
}

impl CsvTable {
    pub fn new(id: String, filepath: String, cfg: CsvConfig) -> Result<Self, StorageError> {
        let schema = Self::infer_arrow_schema(filepath.clone(), &cfg)?;
        let catalog = Self::infer_catalog(id.clone(), id.clone(), &schema);
        Ok(Self {
            id,
            arrow_schema: Arc::new(schema),
            arrow_csv_cfg: cfg,
            filepath,
            catalog,
        })
    }

    fn infer_arrow_schema(filepath: String, cfg: &CsvConfig) -> Result<Schema, StorageError> {
        let schema = reader::infer_schema_from_files(
            &[filepath],
            cfg.delimiter,
            cfg.infer_schema_max_read_records,
            cfg.has_header,
        )?;
        Ok(schema)
    }

    fn infer_catalog(id: String, name: String, schema: &Schema) -> TableCatalog {
        let mut columns = BTreeMap::new();
        let mut column_ids = Vec::new();
        for f in schema.fields().iter() {
            let field_name = f.name().to_string();
            column_ids.push(field_name.clone());
            columns.insert(
                field_name.clone(),
                ColumnCatalog {
                    id: field_name.clone(),
                    desc: ColumnDesc {
                        name: field_name,
                        data_type: f.data_type().clone(),
                    },
                },
            );
        }
        TableCatalog {
            id,
            name,
            columns,
            column_ids,
        }
    }
}

impl Table for CsvTable {
    type TransactionType = CsvTransaction;

    fn read(&self) -> Result<Self::TransactionType, StorageError> {
        CsvTransaction::start(self)
    }
}

pub struct CsvTransaction {
    reader: Reader<File>,
}

impl CsvTransaction {
    pub fn start(table: &CsvTable) -> Result<Self, StorageError> {
        Ok(Self {
            reader: Self::create_reader(
                table.filepath.clone(),
                table.arrow_schema.clone(),
                &table.arrow_csv_cfg,
            )?,
        })
    }

    fn create_reader(
        filepath: String,
        schema: SchemaRef,
        cfg: &CsvConfig,
    ) -> Result<Reader<File>, StorageError> {
        let file = File::open(filepath)?;
        let reader = ReaderBuilder::new(schema)
            .with_batch_size(cfg.batch_size)
            .with_delimiter(cfg.delimiter)
            .with_header(cfg.has_header)
            .build(file)?;
        Ok(reader)
    }
}

impl Transaction for CsvTransaction {
    fn next_batch(&mut self) -> Result<Option<arrow::array::RecordBatch>, StorageError> {
        let batch = self.reader.next().transpose()?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_csv_storage_works() {
        let id = "test".to_string();
        let filepath = "./tests/yellow_tripdata_2019-01.csv".to_string();
        let storage = CsvStorage::default();
        storage.create_table(id.clone(), filepath).unwrap();
        let table = storage.get_table(id.clone()).unwrap();
        let mut tx = table.read().unwrap();
        let batch = tx.next_batch().unwrap();
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 1024);
    }
}
