use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow::datatypes::DataType;

pub type RootCatalogRef = Arc<RootCatalog>;

#[derive(Debug, Clone, Default)]
pub struct RootCatalog {
    pub tables: HashMap<TableId, TableCatalog>,
}

impl RootCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<TableCatalog> {
        self.tables.get(name).cloned()
    }
}

pub type TableId = String;

#[derive(Debug, Clone)]
pub struct TableCatalog {
    pub id: TableId,
    pub name: String,
    /// column ids to keep the order of inferred columns
    pub column_ids: Vec<ColumnId>,
    pub columns: BTreeMap<ColumnId, ColumnCatalog>,
}

impl TableCatalog {
    pub fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalog> {
        self.columns.get(name).cloned()
    }

    pub fn get_all_columns(&self) -> Vec<ColumnCatalog> {
        self.column_ids
            .iter()
            .map(|id| self.columns.get(id).cloned().unwrap())
            .collect()
    }
}

pub type ColumnId = String;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnCatalog {
    pub id: ColumnId,
    pub desc: ColumnDesc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDesc {
    pub name: String,
    pub data_type: DataType,
}
