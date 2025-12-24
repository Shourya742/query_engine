mod array_compute;
mod evaluator;
mod filter;
mod project;
mod table_scan;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use futures::stream::BoxStream;

use crate::executor::filter::FilterExecutor;
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::storage;
use crate::{
    executor::{project::ProjectExecutor, table_scan::TableScanExecutor},
    optimizer::{physical_project::PhysicalProject, PlanRef, PlanTreeNode},
    storage::{StorageError, StorageImpl},
};
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use thiserror::Error;

pub type BoxedExecutor = BoxStream<'static, Result<RecordBatch, ExecutorError>>;

pub struct ExecutorBuilder {
    storage: StorageImpl,
}

impl ExecutorBuilder {
    pub fn new(storage: StorageImpl) -> Self {
        Self { storage }
    }

    pub fn build(&mut self, plan: PlanRef) -> BoxedExecutor {
        self.visit(plan).unwrap()
    }

    pub fn try_collect(&mut self, plan: PlanRef) -> BoxedExecutor {
        self.visit(plan).unwrap()
    }
}

pub async fn try_collect(mut executor: BoxedExecutor) -> Result<Vec<RecordBatch>, ExecutorError> {
    let mut output = Vec::new();
    while let Some(batch) = executor.try_next().await? {
        output.push(batch);
    }
    Ok(output)
}

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),
}

impl PlanVisitor<BoxedExecutor> for ExecutorBuilder {
    fn visit_physical_table_scan(
        &mut self,
        plan: &crate::optimizer::physical_table::PhysicalTableScan,
    ) -> Option<BoxedExecutor> {
        Some(match &self.storage {
            StorageImpl::CsvStorage(storage) => TableScanExecutor {
                plan: plan.clone(),
                storage: storage.clone(),
            }
            .execute(),
            StorageImpl::InMemoryStorage(storage) => TableScanExecutor {
                plan: plan.clone(),
                storage: storage.clone(),
            }
            .execute(),
        })
    }

    fn visit_physical_project(&mut self, plan: &PhysicalProject) -> Option<BoxedExecutor> {
        Some(
            ProjectExecutor {
                exprs: plan.logical().exprs(),
                child: self
                    .visit(plan.children().first().unwrap().clone())
                    .unwrap(),
            }
            .execute(),
        )
    }

    fn visit_physical_filter(
        &mut self,
        plan: &crate::optimizer::physical_filter::PhysicalFilter,
    ) -> Option<BoxedExecutor> {
        Some(
            FilterExecutor {
                expr: plan.logical().expr(),
                child: self
                    .visit(plan.children().first().unwrap().clone())
                    .unwrap(),
            }
            .execute(),
        )
    }
}

mod executor_test {
    use std::sync::Arc;

    use arrow::array::StringArray;

    use crate::{
        binder::Binder,
        executor::{try_collect, ExecutorBuilder},
        optimizer::{
            input_ref_rewriter::InputRefRewriter, physical_rewriter::PhysicalRewriter,
            plan_rewriter::PlanRewriter,
        },
        parser::parse,
        planner::Planner,
        storage::{CsvStorage, Storage, StorageImpl},
    };

    #[tokio::test]
    async fn test_executor_works() {
        let id = "employee".to_string();

        let filepath = "./tests/sample.csv".to_string();
        let storage = CsvStorage::default();
        storage.create_csv_table(id.clone(), filepath).unwrap();

        let stmts = parse("select first_name from employee where id = 1").unwrap();

        let catalog = storage.get_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let bound_stmt = binder.bind(&stmts[0]).unwrap();
        println!("bound_stmt = {bound_stmt:#?}");
        let planner = Planner {};
        let logical_plan = planner.plan(bound_stmt).unwrap();
        println!("logical plan: {logical_plan:#?}");

        let mut input_ref_rewriter = InputRefRewriter::default();
        let new_logical_plan = input_ref_rewriter.rewrite(logical_plan);
        println!("new_logical_plan = {:#?}", new_logical_plan);

        let mut physical_rewriter = PhysicalRewriter {};
        let physical_plan = physical_rewriter.rewrite(new_logical_plan);
        println!("physical_plan = {physical_plan:#?}");
        let mut builder = ExecutorBuilder::new(StorageImpl::CsvStorage(Arc::new(storage)));
        let executor = builder.build(physical_plan);
        let output = try_collect(executor).await.unwrap();
        let a = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(*a, StringArray::from(vec!["Bill", "Gregg", "John", "Von"]));
        println!("output: {output:#?}");
    }
}
