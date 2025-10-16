use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::Field,
};

use crate::binder::expression::BoundExpr;

impl BoundExpr {
    pub fn eval_column(&self, batch: &RecordBatch) -> ArrayRef {
        match &self {
            BoundExpr::InputRef(input_ref) => batch.column(input_ref.index).clone(),
            _ => unimplemented!("expr type {:?} not implemented yet", self),
        }
    }

    pub fn eval_field(&self, batch: &RecordBatch) -> Field {
        match &self {
            BoundExpr::InputRef(input_ref) => batch.schema().field(input_ref.index).clone(),
            _ => unimplemented!("expr type {:?} not implemented yet", self),
        }
    }
}
