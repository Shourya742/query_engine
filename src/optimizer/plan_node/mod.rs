pub mod dummy;
pub mod logical_filter;
pub mod logical_project;
pub mod logical_table_scan;
pub mod plan_node_traits;
use std::fmt::Debug;

use std::sync::Arc;

use crate::catalog::ColumnCatalog;
pub use plan_node_traits::*;

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as `dyn PlanNode`.
/// Meanwhile, we split the trait into lots of sub-traits so that we can easily use macro to impl them.
pub trait PlanNode: WithPlanNodeType + PlanTreeNode + Debug {
    fn schema(&self) -> Vec<ColumnCatalog> {
        vec![]
    }
}

/// The type of reference to a plan node.
pub type PlanRef = Arc<dyn PlanNode>;

/// The core idea of `for_all_plan_nodes` is to generate boilerplate code for all plan nodes,
/// which means passing the name of a macro into another macro.
///
/// We use this pattern to impl a trait for all plan nodes.
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:ident) => {
        $macro! {
            Dummy,
            LogicalTableScan,
            LogicalProject,
            LogicalFilter
        }
    };
}
