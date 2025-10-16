use crate::{
    binder::expression::BoundExpr,
    executor::{array_compute::binary_op, ExecutorError},
    types::build_scalar_value_array,
};
use arrow::compute::cast;
use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::Field,
};

impl BoundExpr {
    pub fn eval_column(&self, batch: &RecordBatch) -> Result<ArrayRef, ExecutorError> {
        match &self {
            BoundExpr::InputRef(input_ref) => Ok(batch.column(input_ref.index).clone()),
            BoundExpr::BinaryOp(expr) => {
                let left = expr.left.eval_column(batch)?;
                let right = expr.right.eval_column(batch)?;
                binary_op(&left, &right, &expr.op)
            }
            BoundExpr::Constant(val) => Ok(build_scalar_value_array(val, batch.num_rows())),
            BoundExpr::ColumnRef(_) => panic!("column ref should be resolved"),
            BoundExpr::TypeCast(tc) => Ok(cast(&tc.expr.eval_column(batch)?, &tc.cast_type)?),
        }
    }

    pub fn eval_field(&self, batch: &RecordBatch) -> Field {
        match &self {
            BoundExpr::InputRef(input_ref) => batch.schema().field(input_ref.index).clone(),
            _ => unimplemented!("expr type {:?} not implemented yet", self),
        }
    }
}

#[cfg(test)]
mod evaluator_test {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };

    use crate::{
        binder::expression::{BoundExpr, BoundInputRef, BoundTypeCast},
        executor::ExecutorError,
    };

    fn build_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_eval_column_for_input_ref() -> Result<(), ExecutorError> {
        let batch = build_record_batch();
        let expr = BoundExpr::InputRef(BoundInputRef {
            index: 1,
            return_type: DataType::Int32,
        });
        let result = expr.eval_column(&batch)?;
        assert_eq!(result.len(), 2);
        assert_eq!(*result, Int32Array::from(vec![3, 4]));
        Ok(())
    }

    #[test]
    fn test_eval_column_fro_type_cast() -> Result<(), ExecutorError> {
        let batch = build_record_batch();
        let expr = BoundExpr::TypeCast(BoundTypeCast {
            expr: Box::new(BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: DataType::Int32,
            })),
            cast_type: DataType::Int64,
        });
        let result = expr.eval_column(&batch)?;
        assert_eq!(result.len(), 2);
        assert_eq!(*result, Int64Array::from(vec![3, 4]));
        Ok(())
    }
}
