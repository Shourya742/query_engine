use arrow::array::RecordBatch;
use futures_async_stream::try_stream;

use crate::{
    binder::expression::BoundExpr,
    executor::{BoxedExecutor, ExecutorError},
};

pub struct ProjectExecutor {
    pub exprs: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl ProjectExecutor {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            yield batch;
        }
    }
}
