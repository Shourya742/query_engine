use crate::for_all_plan_nodes;

use crate::optimizer::physical_project::PhysicalProject;
use crate::optimizer::physical_table::PhysicalTableScan;
use crate::optimizer::plan_node::dummy::Dummy;
use crate::optimizer::plan_node::logical_filter::LogicalFilter;
use crate::optimizer::plan_node::logical_project::LogicalProject;
use crate::optimizer::plan_node::logical_table_scan::LogicalTableScan;
use crate::optimizer::plan_node::physical_filter::PhysicalFilter;
use crate::optimizer::PlanNodeType;
use crate::optimizer::PlanRef;
use paste::paste;

macro_rules! def_rewriter {
    ($($node_name:ident),*) => {
        pub trait PlanVisitor<R> {
            paste! {
                fn visit(&mut self, plan: PlanRef) -> Option<R> {
                    match plan.node_type() {
                        $(
                            PlanNodeType::$node_name => self.[<visit_$node_name:snake>](plan.downcast_ref::<$node_name>().unwrap()),
                        )*
                    }
                }

                $(
                    fn [<visit_$node_name:snake>] (&mut self, plan: &$node_name) -> Option<R> {
                        unimplemented!("The {} is not implemented visitor yet", stringify!($node_name))
                    }
                )*
            }
        }
    };
}

for_all_plan_nodes! { def_rewriter }
