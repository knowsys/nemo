//! RDF imports: loading the statements of an RDF serialization into a predicate
//! with three columns, or four for quads.

use std::sync::Arc;

use nemo_io::{
    file_format::FileFormat,
    rdf::{RdfBatch, RdfReader, RdfReaderOptions, RdfResource},
};
use nemo_physical::{
    dictionary::LabelScope,
    value::{DomainSet, Type, ValueDomain},
};

use crate::engine::compile::translation::{
    Translator,
    imports::{ImportFailure, PendingImport, required_resource},
    options::term_columns,
};

/// Returns the types of the columns of an RDF import with `arity` columns.
///
/// The graph, which comes first in quads, and the subject are IRIs or the nulls of blank
/// nodes.
/// The predicate is an IRI, and the object can be anything.
fn column_types(arity: usize) -> Vec<Type> {
    let node = Type::of_domains(DomainSet::IRI.with(ValueDomain::Null));
    let mut types = vec![node; arity.saturating_sub(2)];
    types.push(Type::of(ValueDomain::Iri));
    types.push(Type::ANY);
    types
}

impl<'a> Translator<'a> {
    /// Returns the label scope of a resource's blank nodes in this translation,
    /// and opens it on first use.
    ///
    /// Every import of the resource in one translation reads the same document,
    /// so the imports agree on its blank nodes.
    /// The next translation reads the resource again, which may have changed in between.
    fn document_scope(&mut self, resource: &str) -> LabelScope {
        if let Some(scope) = self.documents.get(resource) {
            return *scope;
        }
        let scope = self
            .dictionary
            .fresh_label_scope(&format!("document {resource}"));
        self.documents.insert(resource.to_owned(), scope);
        scope
    }

    /// Executes one RDF import (see [execute_imports](Self::execute_imports)).
    pub(super) fn rdf_import(
        &mut self,
        import: &PendingImport,
        fetched: Option<Arc<[u8]>>,
    ) -> Result<(), ImportFailure> {
        let bindings = self.guard_bindings(&import.guards)?;
        let directive = self.plain_directive(import.format, &import.options, &bindings, true)?;
        let resource = required_resource(directive.resource, import)?;
        let variant = self.rdf_variant(import.format, Some(&resource), import.span)?;
        // The directive's `format` keeps or skips source columns,
        // the plan prunes some of the kept ones, and the reader delivers only the rest.
        let kept = term_columns(
            FileFormat::Rdf(Some(variant)),
            directive.formats.as_deref(),
            variant.width(),
            directive.formats_span.or(import.span),
        )?;
        let layout = self.plan.layout(&import.predicate, kept);
        let kept_types: Vec<Type> = column_types(variant.width())
            .into_iter()
            .enumerate()
            .filter(|&(column, _)| !layout.skips(column))
            .map(|(_, column_type)| column_type)
            .collect();
        let mut options = RdfReaderOptions::new(variant);
        options.base = directive.base;
        options.compression = directive.compression;
        options.limit = directive.limit;
        options.selection = self.import_selection(import, &layout)?;
        let table = self.register_import(import, kept_types, &layout)?;
        if self.options_only(import) {
            return Ok(());
        }
        let reader = RdfReader::new(options).with_blank_node_scope(self.document_scope(&resource));
        let input = self.import_input(import, &resource, fetched);

        // The batch parses the statements on worker threads, and a large line-based resource
        // on several of them.
        let loaded = RdfBatch::new(vec![RdfResource { input, reader }])
            .load(self.dictionary, self.context)
            .map_err(ImportFailure::Reading)?
            .pop()
            .expect("a batch gives one load per resource");
        self.absorb(table, layout.delivered(), loaded, &resource, false);
        Ok(())
    }
}
