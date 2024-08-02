//! This module defines [Program].

use super::components::{
    fact::Fact,
    import_export::{ExportDirective, ImportDirective},
    output::Output,
    rule::Rule,
};

/// Representation of a nemo program
#[derive(Debug, Default)]
pub struct Program {
    /// Imported resources
    imports: Vec<ImportDirective>,
    /// Exported resources
    exports: Vec<ExportDirective>,
    /// Rules
    rules: Vec<Rule>,
    /// Facts
    facts: Vec<Fact>,
    /// Outputs
    outputs: Vec<Output>,
}

/// Builder for [Program]s
#[derive(Debug, Default)]
pub struct ProgramBuilder {
    /// The constructed program
    program: Program,
}

impl ProgramBuilder {
    /// Finish building and return a [Program].
    pub fn finalize(self) -> Program {
        self.program
    }

    /// Add a [Rule].
    pub fn add_rule(&mut self, rule: Rule) {
        self.program.rules.push(rule)
    }

    /// Add a [Fact].
    pub fn add_fact(&mut self, fact: Fact) {
        self.program.facts.push(fact);
    }

    /// Add a [ImportDirective].
    pub fn add_import(&mut self, import: ImportDirective) {
        self.program.imports.push(import);
    }

    /// Add a [ExportDirective].
    pub fn add_export(&mut self, export: ExportDirective) {
        self.program.exports.push(export);
    }

    /// Add a [Output].
    pub fn add_output(&mut self, output: Output) {
        self.program.outputs.push(output);
    }
}
