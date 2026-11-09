//! Compiled head atoms, and the existential head of a rule with existential variables.

use nemo_physical::{management::TableId, value::DataValue};

/// One head atom: the table that receives the rule's rows, and the column of each position.
#[derive(Debug)]
pub(crate) struct HeadAtom {
    /// The predicate's table.
    pub(crate) table: TableId,
    /// The rule column at each position.
    ///
    /// The columns are distinct, since a variable written again becomes a computed copy.
    pub(crate) columns: Vec<usize>,
}

/// The existential head of a rule with `!`-variables, evaluated by the restricted chase.
///
/// The *frontier tuple* of a body match is its values for the universal head variables.
/// A frontier tuple is *satisfied* if some values for the existential variables make every
/// head atom hold in the stored tables.
/// For an unsatisfied tuple, each existential variable gets a fresh null,
/// and the [inserts](Self::inserts) are instantiated with these nulls.
///
/// The head atoms without existential variables are also ordinary [HeadAtom]s of the rule.
/// So they receive a row for every new body match, whether its frontier tuple is satisfied or
/// not.
#[derive(Debug)]
pub(crate) struct ExistentialHead {
    /// The rule columns of the universal head variables, in order of first mention.
    pub(crate) frontier: Vec<usize>,
    /// The number of distinct existential variables,
    /// which are numbered in order of first mention.
    pub(crate) existentials: usize,
    /// Every head atom, for the test of whether a frontier tuple is satisfied.
    pub(crate) check: Vec<ExistentialAtom>,
    /// The head atoms with existential variables,
    /// which are instantiated for each unsatisfied frontier tuple.
    pub(crate) inserts: Vec<ExistentialAtom>,
}

/// One atom of the existential head, over frontier columns, existential variables and
/// constants.
#[derive(Debug, Clone)]
pub(crate) struct ExistentialAtom {
    /// The predicate's table.
    pub(crate) table: TableId,
    /// The term at each column.
    pub(crate) columns: Vec<ExistentialTerm>,
}

/// One column of an existential head atom.
#[derive(Debug, Clone)]
pub(crate) enum ExistentialTerm {
    /// A frontier column, by its index in [frontier](ExistentialHead::frontier).
    Frontier(usize),
    /// An existential variable, by its number.
    Existential(usize),
    /// A constant.
    Constant(DataValue),
}
