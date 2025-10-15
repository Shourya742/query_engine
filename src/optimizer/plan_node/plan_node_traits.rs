use crate::for_all_plan_nodes;
use crate::optimizer::physical_project::PhysicalProject;
use crate::optimizer::physical_table::PhysicalTableScan;
use crate::optimizer::plan_node::dummy::Dummy;
use crate::optimizer::plan_node::logical_filter::LogicalFilter;
use crate::optimizer::plan_node::logical_project::LogicalProject;
use crate::optimizer::plan_node::logical_table_scan::LogicalTableScan;
use crate::optimizer::plan_node::PhysicalFilter;
use crate::optimizer::PlanRef;

pub trait WithPlanNodeType {
    fn node_type(&self) -> PlanNodeType;
}

/// impl [`PlanNodeType`] fn for each node.
macro_rules! enum_plan_node_type {
    ($($node_name:ident),*) => {

        /// each enum value represent a PlanNode struct type, helps us to dispatch and downcast
        #[derive(Debug, Clone, PartialEq)]
        pub enum PlanNodeType {
            $($node_name),*
        }
        $(
            impl WithPlanNodeType for $node_name {
                fn node_type(&self) -> PlanNodeType {
                    PlanNodeType::$node_name
                }
            }
        )*
    };
}

for_all_plan_nodes! {enum_plan_node_type}

/// The trait is used by optimizer for rewriting plan nodes.
/// every plan node should implement this trait.
pub trait PlanTreeNode {
    /// Get the child plan nodes.
    fn children(&self) -> Vec<PlanRef>;
    /// Clone the node with new children for rewriting plan node.
    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef;
}
