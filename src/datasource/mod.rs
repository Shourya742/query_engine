use arrow::{datatypes::SchemaRef, error::ArrowError};

mod csv;
pub use csv::*;
use futures::stream::BoxStream;

pub type BoxedRecordBatchStream = BoxStream<'static, Result<RecordBatch, ArrowError>>;

pub trait Datasource {
    fn schema(self: Box<Self>) -> SchemaRef;

    fn execute(self: Box<Self>) -> BoxedRecordBatchStream;
}

#[derive(thiserror::Error, Debug)]
pub enum DataSourceError {
    #[error("arrow error")]
    ArrowErr(#[from] ArrowError),
    #[error("io error")]
    IoError(#[from] std::io::Error),
}
