use std::{fmt, sync::Arc};

use crate::{
    binder::expression::BoundExpr,
    catalog::ColumnCatalog,
    optimizer::{plan_node::PlanRef, PlanNode, PlanTreeNode},
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

    pub fn expr(&self) -> &BoundExpr {
        &self.expr
    }
}

impl PlanNode for LogicalFilter {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.input.schema()
    }
}

impl PlanTreeNode for LogicalFilter {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.input.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 1);
        Arc::new(Self::new(self.expr.clone(), children[0].clone()))
    }
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "LogicalFilter: expr: {:?}", self.expr)
    }
}
