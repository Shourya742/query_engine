use std::collections::HashMap;
pub mod expression;
pub mod statement;
pub mod table;

use sqlparser::ast::{Ident, Statement};

use crate::{
    binder::statement::BoundStatement,
    catalog::{RootCatalogRef, TableCatalog},
};

pub struct Binder {
    catalog: RootCatalogRef,
    context: BinderContext,
}

#[derive(Default)]
struct BinderContext {
    /// Table name == table id
    /// table id -> table catalog
    tables: HashMap<String, TableCatalog>,
}

impl Binder {
    pub fn new(catalog: RootCatalogRef) -> Self {
        Self {
            catalog,
            context: BinderContext::default(),
        }
    }

    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::Query(query) => {
                let bound_select = self.bind_select(query)?;
                Ok(BoundStatement::Select(bound_select))
            }
            _ => Err(BindError::UnsupportedStmt(format!("{stmt:?}"))),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("unsupported statement {0}")]
    UnsupportedStmt(String),
    #[error("invalid table {0}")]
    InvalidTable(String),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("invalid column {0}")]
    InvalidColumn(String),
    #[error("binary operator types mismatch: {0} != {1}")]
    BinaryOpTypeMismatch(String, String),
}

#[cfg(test)]
mod binder_test {
    use std::{assert_matches::assert_matches, collections::BTreeMap, sync::Arc};

    use arrow::datatypes::DataType;

    use crate::{
        binder::{expression::BoundExpr, statement::BoundStatement, Binder},
        catalog::{ColumnCatalog, ColumnDesc, RootCatalog, TableCatalog},
        parser::parse,
    };

    fn build_test_catalog() -> RootCatalog {
        let mut catalog = RootCatalog::new();
        let table_id = "t1".to_string();
        let mut columns = BTreeMap::new();
        columns.insert(
            "c1".to_string(),
            ColumnCatalog {
                id: "c1".to_string(),
                desc: ColumnDesc {
                    name: "c1".to_string(),
                    data_type: DataType::Int32,
                },
            },
        );
        columns.insert(
            "c2".to_string(),
            ColumnCatalog {
                id: "c2".to_string(),
                desc: ColumnDesc {
                    name: "c2".to_string(),
                    data_type: DataType::Int32,
                },
            },
        );
        let column_ids = vec!["c1".to_string(), "c2".to_string()];
        let table_catalog = TableCatalog {
            id: table_id.clone(),
            name: table_id.clone(),
            columns,
            column_ids,
        };
        catalog.tables.insert(table_id, table_catalog);
        catalog
    }

    #[test]
    fn test_bind_select_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select c1, c2 from t1").unwrap();
        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(select.select_list.len(), 2);
                assert_eq!(select.from_table.is_some(), true);
                assert_eq!(select.from_table.unwrap().table_catalog.id, "t1");
            }
        }
    }

    #[test]
    fn test_bind_select_constant_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select 1").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                assert_eq!(select.from_table.is_some(), false);
            }
        }
    }

    #[test]
    fn test_bind_select_agg_func_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select sum(c1), sum(c2) from t1").unwrap();
        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_matches!(select.select_list[0], BoundExpr::AggFunc(..));
                assert_matches!(select.select_list[0], BoundExpr::AggFunc(..));
            }
        }
    }
}
