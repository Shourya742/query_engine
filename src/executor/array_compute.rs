use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    compute::kernels::{
        cmp::{eq, gt, gt_eq, lt, lt_eq},
        numeric::{add, div, mul, sub},
    },
};
use sqlparser::ast::BinaryOperator;

use crate::executor::ExecutorError;
use arrow::array::Float64Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::datatypes::DataType;

macro_rules! compute_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("Compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

macro_rules! arithmetic_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            _ => todo!("unsupported data type"),
        }
    }};
}

pub fn binary_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: &BinaryOperator,
) -> Result<ArrayRef, ExecutorError> {
    match op {
        BinaryOperator::Plus => arithmetic_op!(left, right, add),
        BinaryOperator::Minus => arithmetic_op!(left, right, sub),
        BinaryOperator::Multiply => arithmetic_op!(left, right, mul),
        BinaryOperator::Divide => arithmetic_op!(left, right, div),
        BinaryOperator::Gt => Ok(Arc::new(gt(left, right)?)),
        BinaryOperator::Lt => Ok(Arc::new(lt(left, right)?)),
        BinaryOperator::GtEq => Ok(Arc::new(gt_eq(left, right)?)),
        BinaryOperator::LtEq => Ok(Arc::new(lt_eq(left, right)?)),
        BinaryOperator::Eq => Ok(Arc::new(eq(left, right)?)),
        BinaryOperator::NotEq => todo!(),
        BinaryOperator::And => todo!(),
        BinaryOperator::Or => todo!(),
        _ => todo!(),
    }
}
