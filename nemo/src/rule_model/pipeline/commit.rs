//! This module defines [ProgramCommit].

use crate::rule_model::components::{
    fact::Fact,
    import_export::{ExportDirective, ImportDirective},
    output::Output,
    parameter::ParameterDeclaration,
    rule::Rule,
};

use super::id::ProgramComponentId;

/// Defines a commit within a [super::super::pipeline::ProgramPipeline]
#[derive(Debug, Default)]
pub struct ProgramCommit {
    /// Ids of program components to be deleted
    pub deleted: Vec<ProgramComponentId>,
    /// Ids of program components to be kept
    pub keep: Vec<ProgramComponentId>,

    /// New rules
    pub rules: Vec<Rule>,
    /// New Facts
    pub facts: Vec<Fact>,
    /// New imports
    pub imports: Vec<ImportDirective>,
    /// New exports
    pub exports: Vec<ExportDirective>,
    /// New outputs
    pub outputs: Vec<Output>,
    /// New parameter declaration
    pub parameters: Vec<ParameterDeclaration>,
}

impl ProgramCommit {
    /// Delete a program component.
    pub fn delete(&mut self, id: ProgramComponentId) {
        self.deleted.push(id);
    }

    /// Keep program component
    pub fn keep(&mut self, id: ProgramComponentId) {
        self.keep.push(id);
    }

    /// Add a new [Rule].
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    /// Add a new [Fact].
    pub fn add_fact(&mut self, fact: Fact) {
        self.facts.push(fact);
    }

    /// Add a new [ImportDirective].
    pub fn add_import(&mut self, import: ImportDirective) {
        self.imports.push(import);
    }

    /// Add a new [ExportDirective].
    pub fn add_export(&mut self, export: ExportDirective) {
        self.exports.push(export);
    }

    /// Add a new [Output].
    pub fn add_output(&mut self, output: Output) {
        self.outputs.push(output);
    }

    /// Add a new [ParameterDeclaration].
    pub fn add_parameter(&mut self, parameter: ParameterDeclaration) {
        self.parameters.push(parameter);
    }
}
