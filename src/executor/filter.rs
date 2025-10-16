use arrow::{
    array::{BooleanArray, RecordBatch},
    compute::filter_record_batch,
};
use futures_async_stream::{for_await, try_stream};

use crate::{
    binder::expression::BoundExpr,
    executor::{BoxedExecutor, ExecutorError},
};

pub struct FilterExecutor {
    pub expr: BoundExpr,
    pub child: BoxedExecutor,
}

impl FilterExecutor {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let eval_mark = self.expr.eval_column(&batch)?;
            let predicate = eval_mark
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("filter executor expected evaluate boolean array");
            let batch = filter_record_batch(&batch, predicate)?;
            yield batch;
        }
    }
}
