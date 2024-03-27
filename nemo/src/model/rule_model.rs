//! The data model.

mod aggregate;
pub use aggregate::*;

mod atom;
pub use atom::*;

mod literal;
pub use literal::*;

mod program;
pub use program::*;

mod rule;
pub use rule::*;

mod term;
pub use term::*;

mod predicate;
pub use predicate::*;

mod constraint;
pub use constraint::*;

pub mod import_export;
pub use import_export::*;

mod syntax;
pub(crate) use syntax::*;
