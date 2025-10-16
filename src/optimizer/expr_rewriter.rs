use crate::binder::expression::BoundExpr;

pub trait ExprRewriter {
    fn rewrite_expr(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::Constant(_) => self.rewrite_constant(expr),
            BoundExpr::ColumnRef(_) => self.rewrite_column_ref(expr),
            BoundExpr::InputRef(_) => self.rewrite_input_ref(expr),
            BoundExpr::BinaryOp(_) => self.rewrite_binary_op(expr),
            BoundExpr::TypeCast(_) => self.rewrite_type_cast(expr),
        }
    }

    fn rewrite_constant(&self, _: &mut BoundExpr) {}

    fn rewrite_column_ref(&self, _: &mut BoundExpr) {}

    fn rewrite_input_ref(&self, _: &mut BoundExpr) {}

    fn rewrite_type_cast(&self, _: &mut BoundExpr) {}

    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::BinaryOp(e) => {
                self.rewrite_expr(&mut e.left);
                self.rewrite_expr(&mut e.right);
            }
            _ => unreachable!(),
        }
    }
}
