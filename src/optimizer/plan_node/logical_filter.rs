use std::fmt;

use crate::{binder::expression::BoundExpr, optimizer::plan_node::PlanRef};

pub struct LogicalFilter {
    /// Filtered expression on input PlanRef
    expr: BoundExpr,
    /// the child PlanRef to be be projected.
    _input: PlanRef,
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "LogicalFilter: expr: {:?}", self.expr)
    }
}
