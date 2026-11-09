//! Module names.

use std::fmt::{self, Display, Formatter};

/// The name of the module in a `@use`, such as `graph::tc` or `super::util::pairs`.
///
/// The name is relative to the directory of the file that contains the `@use`.
/// Each `super` goes one directory up,
/// and the other parts are directories followed by the module's file name without `.rls`.
/// If a name has two or more parts and its first part names a
/// [module root](crate::engine::Engine::add_module_root),
/// the name refers to a module inside that root instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModuleName {
    /// The number of `super` parts at the start of the name.
    pub parents: usize,
    /// The parts after the `super` parts: directories, then the file name without `.rls`.
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
