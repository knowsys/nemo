//! This module collects all available transformations

// ...

use super::manager::BigManager;

pub trait ProgramTransformation {
    /// Funktioniert nicht gut mit parsing...
    pub fn transform(manager: &mut BigManager);
}
