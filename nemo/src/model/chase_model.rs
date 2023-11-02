//! A variant of the rule model suitable for computing the chase.

mod aggregate;
pub use aggregate::*;

mod program;
pub use program::*;

mod rule;
pub use rule::*;

mod atom;
pub use atom::*;

mod constructor;
pub use constructor::*;
