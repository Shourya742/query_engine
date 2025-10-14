use arrow::datatypes::SchemaRef;

mod csv;
pub use csv::*;
use futures::stream::BoxStream;

pub type BoxedRecordBatchStream = BoxStream<'static, Result<RecordBatch, ArrowError>>;

pub trait Datasource {
    fn schema(self: Box<Self>) -> SchemaRef;

    fn execute(self: Box<Self>) -> BoxedRecordBatchStream;
}
