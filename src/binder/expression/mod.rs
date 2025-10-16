use arrow::datatypes::DataType;
use itertools::Itertools;
use sqlparser::ast::{BinaryOperator, Expr, Ident};

use crate::{
    binder::{expression::binary_op::BoundBinaryOp, BindError, Binder},
    catalog::ColumnCatalog,
    types::ScalarValue,
};
pub mod binary_op;

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    Constant(ScalarValue),
    ColumnRef(BoundColumnRef),
    InputRef(BoundInputRef),
    BinaryOp(BoundBinaryOp),
}

impl BoundExpr {
    pub fn return_type(&self) -> Option<DataType> {
        match self {
            BoundExpr::Constant(value) => Some(value.data_type()),
            BoundExpr::InputRef(input) => Some(input.return_type.clone()),
            BoundExpr::ColumnRef(column_ref) => {
                Some(column_ref.column_catalog.desc.data_type.clone())
            }
            BoundExpr::BinaryOp(binary_op) => binary_op.return_type.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundColumnRef {
    pub column_catalog: ColumnCatalog,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundInputRef {
    /// column index in data chunk
    pub index: usize,
    pub return_type: DataType,
}

impl Binder {
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Identifier(ident) => self.bind_column_ref_from_identifiers(&[ident.clone()]),
            Expr::CompoundIdentifier(idents) => self.bind_column_ref_from_identifiers(idents),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(left, op, right),
            Expr::UnaryOp { op, expr } => todo!(),
            Expr::Value(v) => Ok(BoundExpr::Constant(v.value.clone().into_string().into())),
            _ => todo!("unsupported expr: {expr:?}"),
        }
    }

    pub fn bind_column_ref_from_identifiers(
        &mut self,
        idents: &[Ident],
    ) -> Result<BoundExpr, BindError> {
        let idents = idents
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect_vec();
        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => return Err(BindError::InvalidTableName(idents)),
        };

        if let Some(table) = table_name {
            let table_catalog = self.context.tables.get(table).unwrap();
            let column_catalog = table_catalog.get_column_by_name(&column_name).unwrap();
            Ok(BoundExpr::ColumnRef(BoundColumnRef { column_catalog }))
        } else {
            let mut got_column = None;
            for (_table_name, table_catalog) in &self.context.tables {
                got_column = Some(table_catalog.get_column_by_name(column_name).unwrap());
            }
            let column_catalog =
                got_column.ok_or_else(|| BindError::InvalidColumn(column_name.clone()))?;
            Ok(BoundExpr::ColumnRef(BoundColumnRef { column_catalog }))
        }
    }
}
