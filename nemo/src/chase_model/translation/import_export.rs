//! This module contains functions for building [ChaseImport]s.

use crate::{
    chase_model::components::{
        export::ChaseExport,
        import::{ChaseImport, ChaseImportClause},
    },
    io::{
        format_builder::ImportExportBuilder,
        formats::{Export, Import},
    },
    rule_model::components::import_export::{
        clause::ImportClause, ExportDirective, ImportDirective,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Build a [ChaseImport] from a given
    /// [ImportDirective][ImportDirective].
    pub(crate) fn build_import(
        &self,
        import_directive: &ImportDirective,
        import_builder: &ImportExportBuilder,
    ) -> ChaseImport {
        let predicate = import_directive.predicate().clone();
        let arity = *self
            .predicate_arity
            .get(&predicate)
            .expect("arity has been determined in validation");

        let handler: Import = import_builder.build_import(predicate.name(), arity);

        ChaseImport::new(predicate, handler)
    }

    /// Build a [ChaseExport] from a given
    /// [ExportDirective][crate::rule_model::components::import_export::ExportDirective].
    pub(crate) fn build_export(
        &self,
        export_directive: &ExportDirective,
        export_builder: &ImportExportBuilder,
    ) -> ChaseExport {
        let predicate = export_directive.predicate().clone();
        let arity = *self
            .predicate_arity
            .get(&predicate)
            .expect("arity has been determined in validation");

        let handler: Export = export_builder.build_export(predicate.name(), arity);

        ChaseExport::new(predicate, handler)
    }

    /// Build a [ChaseImportClause] from a given
    /// [ImportClause][crate::rule_model::components::import_export::clause::ImportClause].
    pub(crate) fn build_import_clause(
        &self,
        import_clause: &ImportClause,
        import_builder: &ImportExportBuilder,
    ) -> ChaseImportClause {
        let predicate = import_clause.import_directive().predicate().clone();
        let arity = *self
            .predicate_arity
            .get(&predicate)
            .expect("arity has been determined in validation");

        let handler: Import = import_builder.build_import(predicate.name(), arity);
        let bindings = import_clause.output_variables().clone();

        ChaseImportClause::new(predicate, handler, bindings)
    }
}
