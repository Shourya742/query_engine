use std::fmt;

use crate::optimizer::{logical_table_scan::LogicalTableScan, PlanNode, PlanTreeNode};

#[derive(Debug, Clone)]
pub struct PhysicalTableScan {
    logical: LogicalTableScan,
}

impl PhysicalTableScan {
    pub fn new(logical: LogicalTableScan) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalTableScan {
        &self.logical
    }
}

impl PlanNode for PhysicalTableScan {
    fn schema(&self) -> Vec<crate::catalog::ColumnCatalog> {
        self.logical.schema()
    }
}

impl PlanTreeNode for PhysicalTableScan {
    fn children(&self) -> Vec<super::PlanRef> {
        self.logical.children()
    }

    fn clone_with_children(&self, children: Vec<super::PlanRef>) -> super::PlanRef {
        self.logical.clone_with_children(children)
    }
}

impl fmt::Display for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalTableScan: table: #{}, columns: [{}]",
            self.logical.table_id(),
            self.logical.column_ids().join(", ")
        )
    }
}
