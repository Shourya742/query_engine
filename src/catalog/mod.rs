use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::DataType;

#[derive(Debug, Clone, Default)]
pub struct RootCatalog {
    pub tables: HashMap<TableId, TableCatalog>,
}

pub type TableId = String;

#[derive(Debug, Clone)]
pub struct TableCatalog {
    pub id: TableId,
    pub name: String,
    pub columns: BTreeMap<ColumnId, ColumnCatalog>,
}

pub type ColumnId = String;

#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    pub id: ColumnId,
    pub desc: ColumnDesc,
}

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name: String,
    pub data_type: DataType,
}
