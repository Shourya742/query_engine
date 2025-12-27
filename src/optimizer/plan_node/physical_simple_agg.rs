use std::fmt;

use crate::optimizer::{logical_agg::LogicalAgg, PlanNode, PlanTreeNode};

#[derive(Debug, Clone)]
pub struct PhysicalSimpleAgg {
    logical: LogicalAgg,
}

impl PhysicalSimpleAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalAgg {
        &self.logical
    }
}

impl PlanNode for PhysicalSimpleAgg {
    fn schema(&self) -> Vec<crate::catalog::ColumnCatalog> {
        self.logical().schema()
    }
}

impl PlanTreeNode for PhysicalSimpleAgg {
    fn children(&self) -> Vec<super::PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<super::PlanRef>) -> super::PlanRef {
        self.logical().clone_with_children(children)
    }
}

impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "PhysicalSimpleAgg: agg_funcs {:?} group_by {:?}",
            self.logical().agg_funcs(),
            self.logical().group_by()
        )
    }
}
