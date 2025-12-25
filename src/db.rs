use std::sync::Arc;

use arrow::{array::RecordBatch, error::ArrowError};
use sqlparser::parser::ParserError;
use thiserror::Error;

use crate::{
    binder::{BindError, Binder},
    executor::{try_collect, ExecutorBuilder, ExecutorError},
    optimizer::{
        input_ref_rewriter::InputRefRewriter, physical_rewriter::PhysicalRewriter,
        plan_rewriter::PlanRewriter,
    },
    parser::parse,
    planner::{LogicalPlanError, Planner},
    storage::{CsvStorage, Storage, StorageError, StorageImpl},
};

pub struct Database {
    storage: StorageImpl,
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("parse error: {0}")]
    Parse(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("bind error: {0}")]
    Bind(
        #[source]
        #[from]
        BindError,
    ),
    #[error("logical plan error: {0}")]
    Plan(
        #[source]
        #[from]
        LogicalPlanError,
    ),
    #[error("execute error: {0}")]
    Execute(
        #[source]
        #[from]
        ExecutorError,
    ),
    #[error("Storage error: {0}")]
    StorageError(
        #[source]
        #[from]
        #[backtrace]
        StorageError,
    ),
    #[error("Arrow error: {0}")]
    ArrowError(
        #[source]
        #[from]
        #[backtrace]
        ArrowError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl Database {
    pub fn new_on_csv() -> Self {
        let storage = Arc::new(CsvStorage::new());
        Database {
            storage: StorageImpl::CsvStorage(storage),
        }
    }

    pub fn create_csv_table(
        &self,
        table_name: String,
        filepath: String,
    ) -> Result<(), DatabaseError> {
        if let StorageImpl::CsvStorage(ref storage) = self.storage {
            storage.create_csv_table(table_name, filepath)?;
            Ok(())
        } else {
            Err(DatabaseError::InternalError(
                "currently only support csv storage".to_string(),
            ))
        }
    }

    pub async fn run(&self, sql: &str) -> Result<Vec<RecordBatch>, DatabaseError> {
        let storage = if let StorageImpl::CsvStorage(ref storage) = self.storage {
            storage
        } else {
            return Err(DatabaseError::InternalError(
                "currently only support csv storage".to_string(),
            ));
        };

        let stats = parse(sql)?;
        let catalog = storage.get_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let bound_stmt = binder.bind(&stats[0])?;
        println!("bound_stmt = {:#?}", bound_stmt);

        let planner = Planner {};
        let logical_plan = planner.plan(bound_stmt)?;
        println!("logical_plan = {:#?}", logical_plan);
        let mut input_ref_rewriter = InputRefRewriter::default();
        let new_logical_plan = input_ref_rewriter.rewrite(logical_plan);
        println!("new_logical_plan = {:#?}", new_logical_plan);

        let mut physical_rewriter = PhysicalRewriter {};
        let physical_plan = physical_rewriter.rewrite(new_logical_plan);
        println!("Physical plan = {:#?}", physical_plan);

        let mut builder = ExecutorBuilder::new(StorageImpl::CsvStorage(storage.clone()));
        let executor = builder.build(physical_plan);

        let output = try_collect(executor).await?;
        Ok(output)
    }
}
