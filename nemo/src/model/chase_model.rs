//! A variant of the rule model suitable for computing the chase.

mod aggregate;
pub(crate) use aggregate::*;

mod program;
pub(crate) use program::*;

mod rule;
pub(crate) use rule::*;

mod atom;
pub(crate) use atom::*;
// TODO: pub needed in Python bindings. This should change.
pub use atom::{ChaseAtom, ChaseFact};

mod constructor;
pub(crate) use constructor::*;

pub(crate) mod variable;
