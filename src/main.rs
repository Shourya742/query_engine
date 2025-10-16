#![feature(yield_expr)]
#![feature(coroutines)]
#![feature(iterator_try_collect)]
#![allow(warnings)]
use std::sync::Arc;

use crate::{
    binder::Binder,
    executor::{try_collect, ExecutorBuilder},
    optimizer::{
        input_ref_rewriter::InputRefRewriter, physical_rewriter::PhysicalRewriter,
        plan_rewriter::PlanRewriter,
    },
    parser::parse,
    planner::Planner,
    storage::{CsvStorage, Storage, Table, Transaction},
};
use anyhow::Result;

mod binder;
mod catalog;
mod executor;
mod optimizer;
mod parser;
mod planner;
mod storage;
mod types;

#[tokio::main]
async fn main() -> Result<()> {
    let id = "employee".to_string();
    let filepath = "./tests/sample.csv".to_string();
    let storage = CsvStorage::default();
    storage.create_table(id.clone(), filepath)?;
    let stats = parse("select first_name from employee where last_name = 'Hopkins'").unwrap();
    let catalog = storage.get_catalog();
    println!("catalog = {:#?}", catalog);
    let mut binder = Binder::new(Arc::new(catalog));
    let bound_stmt = binder.bind(&stats[0]).unwrap();
    println!("bound_stmt = {:#?}", bound_stmt);
    let planner = Planner {};
    let logical_plan = planner.plan(bound_stmt)?;
    println!("logical_plan = {:#?}", logical_plan);

    let mut input_ref_rewriter = InputRefRewriter::default();
    let new_logical_plan = input_ref_rewriter.rewrite(logical_plan);
    println!("new logical_path: {new_logical_plan:#?}");

    let mut physical_rewriter = PhysicalRewriter {};
    let physical_plan = physical_rewriter.rewrite(new_logical_plan);
    println!("physical_plan = {:#?}", physical_plan);
    let mut builder = ExecutorBuilder::new(storage::StorageImpl::CsvStorage(Arc::new(storage)));
    let executor = builder.build(physical_plan);
    let output = try_collect(executor).await?;
    println!("Output: {output:#?}");
    Ok(())
}
