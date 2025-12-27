use crate::for_all_plan_nodes;
use crate::optimizer::physical_project::PhysicalProject;
use crate::optimizer::physical_table::PhysicalTableScan;
use crate::optimizer::plan_node::dummy::Dummy;
use crate::optimizer::plan_node::logical_filter::LogicalFilter;
use crate::optimizer::plan_node::logical_project::LogicalProject;
use crate::optimizer::plan_node::logical_table_scan::LogicalTableScan;
use crate::optimizer::plan_node::physical_filter::PhysicalFilter;
use crate::optimizer::plan_node::LogicalAgg;
use crate::optimizer::plan_node::PhysicalSimpleAgg;
use crate::optimizer::PlanRef;
use crate::optimizer::{PlanNodeType, PlanTreeNode};
use itertools::Itertools;
use paste::paste;

macro_rules! def_rewriter {
    ($($node_name: ident),*) => {
        pub trait PlanRewriter {
            paste!{
                fn rewrite(&mut self, plan: PlanRef) -> PlanRef {
                    match plan.node_type() {
                        $(
                            PlanNodeType::$node_name => self.[<rewrite_$node_name:snake>](plan.downcast_ref::<$node_name>().unwrap()),
                        )*
                    }
                }

                $(
                    fn [<rewrite_$node_name:snake>](&mut self, plan: &$node_name) -> PlanRef {
                        let new_children = plan.children().into_iter().map(|child| self.rewrite(child.clone())).collect_vec();
                        plan.clone_with_children(new_children)
                    }
                )*
            }
        }
    };
}

for_all_plan_nodes!(def_rewriter);
