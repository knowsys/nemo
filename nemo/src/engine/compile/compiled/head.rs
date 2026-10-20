//! Head atoms, and the existential head the restricted chase evaluates.

use nemo_physical::{management::TableId, value::DataValue};

/// One head atom: a table receiving rows built from the rule's columns.
#[derive(Debug)]
pub(crate) struct HeadAtom {
    /// The predicate's table.
    pub(crate) table: TableId,
    /// The rule column at each position, distinct by normalization.
    pub(crate) columns: Vec<usize>,
}

/// The existential head of a rule with `!`-variables, evaluated by the restricted chase.
///
/// Satisfaction is checked against the stored tables, without further state.
/// Per application, the delta of body matches is projected to the frontier.
/// A frontier tuple is *satisfied* if one assignment of all existential variables makes every
/// head atom hold.
/// Unsatisfied tuples receive one fresh null per existential variable,
/// which instantiates the insert atoms.
/// Head atoms without existential variables stay ordinary [HeadAtom]s,
/// which fire for every delta match, whether or not its frontier tuple is satisfied.
#[derive(Debug)]
pub(crate) struct ExistentialHead {
    /// The frontier: the rule columns of the universal head
    /// variables, in first-mention order.
    pub(crate) frontier: Vec<usize>,
    /// Number of distinct existential variables, indexed in
    /// first-occurrence order.
    pub(crate) existentials: usize,
    /// The satisfaction check: every head atom.
    pub(crate) check: Vec<ExistentialAtom>,
    /// The atoms instantiated per unsatisfied tuple, the head atoms carrying existential
    /// variables.
    pub(crate) inserts: Vec<ExistentialAtom>,
}

/// One atom of the existential head, over frontier columns, existential
/// variables, and constants.
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
    /// A frontier column, by index into the frontier list.
    Frontier(usize),
    /// An existential variable, by index.
    Existential(usize),
    /// A ground value.
    Constant(DataValue),
}
