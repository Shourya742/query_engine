use core::fmt;

use crate::{
    binder::expression::BoundExpr,
    optimizer::{plan_node::PlanRef, PlanNode},
};

#[derive(Debug, Clone)]
pub struct LogicalProject {
    /// evaluated projection expressions on input PlanRef
    pub exprs: Vec<BoundExpr>,
    /// The child PlanRef to be projected
    pub input: PlanRef,
}

impl LogicalProject {
    pub fn new(exprs: Vec<BoundExpr>, input: PlanRef) -> Self {
        Self { exprs, input }
    }
}

impl PlanNode for LogicalProject {
    fn schema(&self) -> Vec<crate::catalog::ColumnCatalog> {
        self.input.schema()
    }
}

impl fmt::Display for LogicalProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "LogicalProject: exprs: {:?}", self.exprs)
    }
}
