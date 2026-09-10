use std::fmt;

use super::bitset::BitSet;

#[derive(Debug)]
pub struct Constraint<'a> {
    /// Which relation (edge color)
    pub relation: usize,

    /// edge of H
    pub vars: &'a [usize],

    /// Remaining compatible tuples
    pub alive: BitSet,
}

impl<'a> fmt::Display for Constraint<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Constraint {{ color={}, vars={:?}, alive={} }}",
            self.relation, self.vars, self.alive
        )
    }
}
