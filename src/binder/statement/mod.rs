use crate::binder::{expression::BoundExpr, table::BoundTableRef, BindError, Binder};
use sqlparser::ast::SetExpr::Select;
use sqlparser::ast::{Query, SelectItem};

#[derive(Debug)]
pub enum BoundStatement {
    Select(BoundSelect),
}

#[derive(Debug)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    pub from_table: Option<BoundTableRef>,
    pub where_clause: Option<BoundExpr>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<BoundSelect, BindError> {
        let select = match query.body.as_ref() {
            Select(select) => select,
            _ => todo!(),
        };

        let from_table = if select.from.is_empty() {
            None
        } else {
            Some(self.bind_table_with_joins(&select.from[0])?)
        };

        let mut select_list = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr: _, alias: _ } => todo!(),
                SelectItem::QualifiedWildcard(_, _) => todo!(),
                SelectItem::Wildcard(_) => todo!(),
            }
        }

        let where_clause = select
            .selection
            .as_ref()
            .map(|expr| self.bind_expr(expr))
            .transpose()?;

        Ok(BoundSelect {
            select_list,
            from_table,
            where_clause,
        })
    }
}
