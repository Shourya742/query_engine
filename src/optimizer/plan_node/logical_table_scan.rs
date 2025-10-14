use std::fmt;

use itertools::Itertools;

use crate::{
    catalog::{ColumnCatalog, TableId},
    optimizer::plan_node::PlanNode,
};
pub struct LogicalTableScan {
    table_id: TableId,
    columns: Vec<ColumnCatalog>,
}

impl PlanNode for LogicalTableScan {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.columns.clone()
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
