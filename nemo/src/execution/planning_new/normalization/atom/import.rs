//! This module defines [ImportAtom].

use std::fmt::Display;

use crate::{
    execution::planning_new::normalization::import::ImportInstruction,
    io::{format_builder::ImportExportBuilder, formats::Import},
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

/// Represents an import that is executed as part of a rule evaluation
#[derive(Debug, Clone)]
pub struct ImportAtom {
    /// Import instruction
    import: ImportInstruction,

    /// List of variables bound to values
    /// that will be input for the input
    variables: Vec<Variable>,
}

impl Display for ImportAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let terms = DisplaySeperatedList::display(
            self.variables.iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );
        let predicate = self.predicate();

        f.write_str(&format!(
            "{predicate}{}{terms}{}",
            syntax::expression::atom::OPEN,
            syntax::expression::atom::CLOSE
        ))
    }
}

impl ImportAtom {
    /// Create a new [ImportAtom].
    pub fn new(predicate: Tag, handler: Import, bindings: Vec<Variable>) -> Self {
        Self {
            import: ImportInstruction::new(predicate, handler),
            variables: bindings,
        }
    }

    /// Return the predicate.
    pub fn predicate(&self) -> Tag {
        self.import.predicate()
    }

    /// Return the arity of this atom.
    pub fn arity(&self) -> usize {
        self.variables.len()
    }

    /// Return the handler.
    pub fn handler(&self) -> Import {
        self.import.handler()
    }

    /// Return an iterator over the variables.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.variables.iter()
    }

    /// Return a (cloned) list of the contained variables.
    pub fn variables_cloned(&self) -> Vec<Variable> {
        self.variables.clone()
    }
}

impl ImportAtom {
    /// Receives a [crate::rule_model::components::import_export::clause::ImportClause]
    /// and normalzes it to an [ImportAtom]
    pub fn normalize_import(
        builder: &ImportExportBuilder,
        import: &crate::rule_model::components::import_export::clause::ImportClause,
    ) -> Self {
        let predicate = import.predicate().clone();
        let arity = import.output_variables().len();

        let handler: Import = builder.build_import(
            predicate.name(),
            import
                .import_directive()
                .expected_input_arity()
                .unwrap_or(arity),
            import
                .import_directive()
                .expected_output_arity()
                .unwrap_or(arity),
            Vec::new(),
        );
        let variables = import.output_variables().clone();

        Self {
            import: ImportInstruction::new(predicate, handler),
            variables,
        }
    }
}
