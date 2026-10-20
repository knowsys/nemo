//! Module names.

use std::fmt::{self, Display, Formatter};

/// The name a `@use` gives its module: `graph::tc`, `super::util::pairs`.
///
/// The name is relative to the directory of the file containing the `@use`:
/// each `super` goes one directory up,
/// and the other parts are directories followed by the module's file name without `.rls`.
/// A name of two or more parts whose first part is a module root
/// names a module inside that root instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModuleName {
    /// How many `super` steps the name begins with.
    pub parents: usize,
    /// The parts after the `super` steps: directories, then the file name without `.rls`.
    pub segments: Vec<String>,
}

impl Display for ModuleName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for _ in 0..self.parents {
            f.write_str("super::")?;
        }
        f.write_str(&self.segments.join("::"))
    }
}
