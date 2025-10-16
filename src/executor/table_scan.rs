use std::sync::Arc;

use futures_async_stream::try_stream;

use crate::executor::ExecutorError;
use crate::{
    optimizer::physical_table::PhysicalTableScan,
    storage::{Storage, Table, Transaction},
};
use arrow::record_batch::RecordBatch;

pub struct TableScanExecutor<S: Storage> {
    pub plan: PhysicalTableScan,
    pub storage: Arc<S>,
}

impl<S: Storage> TableScanExecutor<S> {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        let table_id = self.plan.logical().table_id();
        let table = self.storage.get_table(table_id)?;
        let mut tx = table.read()?;
        loop {
            match tx.next_batch() {
                Ok(batch) => {
                    if let Some(batch) = batch {
                        yield batch;
                    } else {
                        break;
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}
