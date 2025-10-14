use std::fmt;

pub struct Dummy {}

impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Dummy:")
    }
}
