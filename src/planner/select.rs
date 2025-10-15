use std::sync::Arc;

use crate::{
    binder::statement::BoundSelect,
    optimizer::{
        logical_filter::LogicalFilter, logical_project::LogicalProject,
        logical_table_scan::LogicalTableScan, PlanRef,
    },
    planner::{LogicalPlanError, Planner},
};

impl Planner {
    pub fn plan_select(&self, stmt: BoundSelect) -> Result<PlanRef, LogicalPlanError> {
        let mut plan: PlanRef;

        if let Some(table_ref) = stmt.from_table {
            plan = Arc::new(LogicalTableScan::new(
                table_ref.table_catalog.id.clone(),
                table_ref.table_catalog.get_all_columns(),
            ))
        } else {
            todo!("need logical values")
        }

        if let Some(expr) = stmt.where_clause {
            plan = Arc::new(LogicalFilter::new(expr, plan));
        }

        if !stmt.select_list.is_empty() {
            plan = Arc::new(LogicalProject::new(stmt.select_list, plan))
        }

        Ok(plan)
    }
}
