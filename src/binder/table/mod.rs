use sqlparser::ast::{TableFactor, TableWithJoins};

use crate::{
    binder::{BindError, Binder},
    catalog::TableCatalog,
};

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";
pub static INTERNAL_SCHEMA_NAME: &str = "pg_catalog";

#[derive(Debug)]
pub struct BoundTableRef {
    pub table_catalog: TableCatalog,
}

impl Binder {
    pub fn bind_table_with_joins(
        &mut self,
        table_with_joins: &TableWithJoins,
    ) -> Result<BoundTableRef, BindError> {
        self.bind_table_ref(&table_with_joins.relation)
    }

    pub fn bind_table_ref(&mut self, table: &TableFactor) -> Result<BoundTableRef, BindError> {
        match table {
            TableFactor::Table { name, alias: _, .. } => {
                let (_database, _schema, table) = match name.0.as_slice() {
                    [table] => (
                        DEFAULT_DATABASE_NAME.to_string(),
                        DEFAULT_SCHEMA_NAME.to_string(),
                        table.as_ident().unwrap().to_string(),
                    ),
                    [schema, table] => (
                        DEFAULT_DATABASE_NAME.to_string(),
                        schema.as_ident().unwrap().to_string(),
                        table.as_ident().unwrap().to_string(),
                    ),
                    [db, schema, table] => (
                        db.as_ident().unwrap().to_string(),
                        schema.as_ident().unwrap().to_string(),
                        table.as_ident().unwrap().to_string(),
                    ),
                    _ => return Err(BindError::InvalidTable(name.to_string())),
                };

                let table_name = table.to_string();
                let table_catalog = self
                    .catalog
                    .get_table_by_name(&table_name)
                    .ok_or_else(|| BindError::InvalidTable(table_name.clone()))?;
                self.context
                    .tables
                    .insert(table_name, table_catalog.clone());
                Ok(BoundTableRef { table_catalog })
            }
            _ => panic!("unsupported table factor"),
        }
    }
}
