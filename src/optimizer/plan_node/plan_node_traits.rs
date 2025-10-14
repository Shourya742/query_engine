use crate::for_all_plan_nodes;
use crate::optimizer::plan_node::dummy::Dummy;
use crate::optimizer::plan_node::logical_table_scan::LogicalTableScan;

pub trait WithPlanNodeType {
    fn node_type(&self) -> PlanNodeType;
}

macro_rules! enum_plan_node_type {
    ($($node_name:ident),*) => {
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
