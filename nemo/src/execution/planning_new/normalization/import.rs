//! This module defines [ImportInstruction].

use crate::{
    execution::planning_new::normalization::rule::NormalizedRule, io::formats::Import,
    rule_model::components::tag::Tag,
};

/// Component for handling imports
#[derive(Debug, Clone)]
pub struct ImportInstruction {
    /// Predicate that will contain the data
    predicate: Tag,
    /// Handler object responsible for importing data
    handler: Import,
}

impl ImportInstruction {
    /// Create a new [ImportInstruction].
    pub fn new(predicate: Tag, handler: Import) -> Self {
        Self { predicate, handler }
    }

    /// Return the predicate.
    pub fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    /// Return the handler.
    pub fn handler(&self) -> Import {
        self.handler.clone()
    }

    /// Return the arity of this import.
    pub fn arity(&self) -> usize {
        self.handler.predicate_arity()
    }
}

impl ImportInstruction {
    /// Receives a [crate::rule_model::components::import_export::ImportDirective]
    /// and normalizes into a [ImportInstruction].
    ///
    /// # Panics
    /// Pancis if the program is ill-formed and hence the arity of this import
    /// cannot be deduced.
    pub fn normalize_import(
        import: &crate::rule_model::components::import_export::ImportDirective,
        arity: Option<usize>,
    ) -> Self {
        let import_builder = import.builder().expect("invalid import directive");

        let predicate = import.predicate().clone();
        let filter_rules = import
            .filter_rules()
            .iter()
            .map(|rule| NormalizedRule::normalize_rule(rule, 0))
            .collect::<Vec<_>>();

        let input_arity = import
            .expected_input_arity()
            .or(arity)
            .expect("predicate has unknown arity");
        let output_arity = import
            .expected_output_arity()
            .or(arity)
            .expect("predicate has unknown arity");

        let handler: Import =
            import_builder.build_import(predicate.name(), input_arity, output_arity, filter_rules);

        Self::new(predicate, handler)
    }
}
