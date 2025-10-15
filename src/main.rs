use std::sync::Arc;

use crate::{
    binder::Binder,
    parser::parse,
    planner::Planner,
    storage::{CsvStorage, Storage, Table, Transaction},
};
use anyhow::Result;

mod binder;
mod catalog;
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
    let table = storage.get_table(id)?;
    let mut tx = table.read()?;

    let mut total_cnt = 0;
    loop {
        let batch = tx.next_batch()?;
        match batch {
            Some(batch) => total_cnt += batch.num_rows(),
            None => break,
        }
    }
    println!("total_cnt = {:?}", total_cnt);
    let catalog = storage.get_catalog();
    println!("catalog = {:#?}", catalog);
    let mut binder = Binder::new(Arc::new(catalog));
    let stats = parse("select first_name from employee where last_name = 'Hopkins'").unwrap();
    let bound_stmt = binder.bind(&stats[0]).unwrap();
    println!("bound_stmt = {:#?}", bound_stmt);
    let planner = Planner {};
    let logical_plan = planner.plan(bound_stmt)?;
    println!("logical_plan = {:#?}", logical_plan);

    Ok(())
}
