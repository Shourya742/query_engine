use std::fmt;

use crate::optimizer::{logical_project::LogicalProject, PlanNode, PlanRef, PlanTreeNode};

#[derive(Debug, Clone)]
pub struct PhysicalProject {
    logical: LogicalProject,
}

impl PhysicalProject {
    pub fn new(logical: LogicalProject) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalProject {
        &self.logical
    }
}

impl PlanNode for PhysicalProject {
    fn schema(&self) -> Vec<crate::catalog::ColumnCatalog> {
        self.logical().schema()
    }
}

impl PlanTreeNode for PhysicalProject {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        self.logical().clone_with_children(children)
    }
}

impl fmt::Display for PhysicalProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalProject: exprs {:?}", self.logical().exprs())
    }
}
