use std::fmt;

use crate::{
    binder::expression::BoundExpr,
    catalog::ColumnCatalog,
    optimizer::{plan_node::PlanRef, PlanNode},
};

#[derive(Debug, Clone)]
pub struct LogicalFilter {
    /// Filtered expression on input PlanRef
    expr: BoundExpr,
    /// the child PlanRef to be be projected.
    input: PlanRef,
}

impl LogicalFilter {
    pub fn new(expr: BoundExpr, input: PlanRef) -> Self {
        Self { expr, input }
    }
}

impl PlanNode for LogicalFilter {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.input.schema()
    }
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "LogicalFilter: expr: {:?}", self.expr)
    }
}
