//! This module defines [ExportInstruction].

use crate::{
    execution::planning_new::normalization::rule::NormalizedRule, io::formats::Export,
    rule_model::components::tag::Tag,
};

/// Component for handling exports
#[derive(Debug, Clone)]
pub struct ExportInstruction {
    /// Predicate that will contain the data
    predicate: Tag,
    /// Handler object responsible for exporting data
    handler: Export,
}

impl ExportInstruction {
    /// Create a new [ExportInstruction].
    pub fn new(predicate: Tag, handler: Export) -> Self {
        Self { predicate, handler }
    }

    /// Return the predicate.
    pub fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    /// Return the [Export] handler
    pub fn handler(&self) -> Export {
        self.handler.clone()
    }

    /// Return the arity of this import.
    pub fn arity(&self) -> usize {
        self.handler.predicate_arity()
    }
}

impl ExportInstruction {
    /// Receives a [crate::rule_model::components::import_export::ExportDirective]
    /// and normalizes into a [ExportInstruction].
    ///
    /// # Panics
    /// Pancis if the program is ill-formed and hence the arity of this export
    /// cannot be deduced.
    pub fn normalize_import(
        export: &crate::rule_model::components::import_export::ExportDirective,
        arity: Option<usize>,
    ) -> Self {
        let export_builder = export.builder().expect("invalid export");
        let predicate = export.predicate().clone();

        let filter_rules = export
            .filter_rules()
            .iter()
            .map(|rule| NormalizedRule::normalize_rule(rule, 0))
            .collect::<Vec<_>>();

        let export_arity = export_builder
            .expected_arity()
            .or(arity)
            .expect("predicate has unknown arity");

        let handler: Export =
            export_builder.build_export(predicate.name(), export_arity, filter_rules);

        Self::new(predicate, handler)
    }
}
