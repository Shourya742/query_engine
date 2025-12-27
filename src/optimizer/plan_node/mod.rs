pub mod dummy;
pub mod logical_agg;
pub mod logical_filter;
pub mod logical_project;
pub mod logical_table_scan;
pub mod physical_filter;
pub mod physical_project;
pub mod physical_simple_agg;
pub mod physical_table;
pub mod plan_node_traits;
pub use crate::optimizer::logical_agg::*;
use crate::optimizer::physical_filter::PhysicalFilter;
use crate::optimizer::physical_project::PhysicalProject;
pub use crate::optimizer::physical_simple_agg::*;
use crate::optimizer::physical_table::PhysicalTableScan;
use crate::optimizer::plan_node::dummy::Dummy;
use crate::optimizer::plan_node::logical_filter::LogicalFilter;
use crate::optimizer::plan_node::logical_project::LogicalProject;
use crate::optimizer::plan_node::logical_table_scan::LogicalTableScan;
use paste::paste;
use std::fmt::Debug;

use std::sync::Arc;

use crate::catalog::ColumnCatalog;
use downcast_rs::{impl_downcast, Downcast};
pub use plan_node_traits::*;

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as `dyn PlanNode`.
/// Meanwhile, we split the trait into lots of sub-traits so that we can easily use macro to impl them.
pub trait PlanNode: WithPlanNodeType + PlanTreeNode + Debug + Downcast + Send + Sync {
    fn schema(&self) -> Vec<ColumnCatalog> {
        vec![]
    }
}

impl_downcast!(PlanNode);

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
            LogicalFilter,
            LogicalAgg,
            PhysicalFilter,
            PhysicalTableScan,
            PhysicalProject,
            PhysicalSimpleAgg
        }
    };
}

macro_rules! impl_downcast_utility {
    ($($node_name:ident),*) => {
        impl dyn PlanNode {
            $(
                paste! {
                    pub fn [<as_$node_name:snake>] (&self) -> std::result::Result<&$node_name, ()> {
                        self.downcast_ref::<$node_name>().ok_or(())
                    }
                }
            )*
        }
    };
}

for_all_plan_nodes! {impl_downcast_utility}
