//! This module defines [Program].

use super::component::{
    fact::Fact,
    import_export::{ExportDirective, ImportDirective},
    rule::Rule,
};

/// Representation of a nemo program
#[derive(Debug)]
pub struct Program {
    /// Imported resources
    imports: Vec<ImportDirective>,
    /// Exported resources
    exports: Vec<ExportDirective>,
    /// Rules
    rules: Vec<Rule>,
    /// Facts
    facts: Vec<Fact>,
}

impl Program {
    pub fn from_ast() -> Self {
        todo!()
    }
}
