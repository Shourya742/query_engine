use crate::storage::{CsvStorage, Storage, Table, Transaction};
use anyhow::Result;

mod binder;
mod catalog;
mod parser;
mod storage;
mod types;

#[tokio::main]
async fn main() -> Result<()> {
    let id = "test".to_string();
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
    Ok(())
}
