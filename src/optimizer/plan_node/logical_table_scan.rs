use std::{fmt, sync::Arc};

use itertools::Itertools;

use crate::{
    catalog::{ColumnCatalog, TableId},
    optimizer::{plan_node::PlanNode, PlanTreeNode},
};

#[derive(Debug, Clone)]
pub struct LogicalTableScan {
    table_id: TableId,
    columns: Vec<ColumnCatalog>,
}

impl LogicalTableScan {
    pub fn new(table_id: TableId, columns: Vec<ColumnCatalog>) -> Self {
        Self { table_id, columns }
    }
}

impl PlanNode for LogicalTableScan {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.columns.clone()
    }
}

impl PlanTreeNode for LogicalTableScan {
    fn children(&self) -> Vec<super::PlanRef> {
        vec![]
    }

    fn clone_with_children(&self, children: Vec<super::PlanRef>) -> super::PlanRef {
        assert!(children.is_empty());
        Arc::new(self.clone())
    }
}

impl fmt::Display for LogicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalTableScan: table: #{}, columns: [{}]",
            self.table_id,
            self.columns.iter().map(|c| c.id.clone()).join(", ")
        )
    }
}
