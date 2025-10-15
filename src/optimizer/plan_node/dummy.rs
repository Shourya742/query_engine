use std::fmt;

use crate::optimizer::PlanNode;

#[derive(Debug, Clone)]
pub struct Dummy {}

impl PlanNode for Dummy {}

impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Dummy:")
    }
}
