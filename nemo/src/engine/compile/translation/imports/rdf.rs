//! RDF imports: a serialization's statements into a predicate of the
//! variant's arity.

use std::sync::Arc;

use nemo_io::{
    file_format::FileFormat,
    load::Reader,
    rdf::{RdfOptions, RdfReader},
};
use nemo_physical::{
    dictionary::LabelScope,
    value::{DomainSet, Type, ValueDomain},
};

use crate::{
    diagnostics::AboutResource,
    engine::compile::translation::{
        Translator,
        imports::{ImportFailure, PendingImport, required_resource},
        options::term_columns,
    },
};

/// Returns the types an RDF import's columns hold:
/// the graph (quads, first) and the subject are IRIs or blank-node nulls,
/// the predicate an IRI, the object anything.
fn column_types(arity: usize) -> Vec<Type> {
    let node = Type::of_domains(DomainSet::IRI.with(ValueDomain::Null));
    let mut types = vec![node; arity.saturating_sub(2)];
    types.push(Type::of(ValueDomain::Iri));
    types.push(Type::ANY);
    types
}

impl<'a> Translator<'a> {
    /// Returns the label scope of a resource's blank nodes in this translation,
    /// opened on first use: every import of the resource in one loading call reads the same
    /// document, so they agree on its blank nodes,
    /// while the next call reads the resource anew, which may have changed.
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
        // The directive's `format` keeps or skips source columns, and
        // the plan prunes among the kept; the reader reads only the rest.
        let kept = term_columns(
            FileFormat::Rdf(Some(variant)),
            directive.formats.as_deref(),
            variant.arity(),
            directive.formats_span.or(import.span),
        )?;
        let layout = self.plan.layout(&import.predicate, kept);
        let kept_types: Vec<Type> = column_types(variant.arity())
            .into_iter()
            .enumerate()
            .filter(|&(column, _)| !layout.skips(column))
            .map(|(_, column_type)| column_type)
            .collect();
        let mut options = RdfOptions::new(variant);
        options.base = directive.base;
        options.compression = directive.compression;
        options.limit = directive.limit;
        options.selection = self.import_selection(import, &layout)?;
        let table = self.register_import(import, kept_types, &layout)?;
        if self.options_only(import) {
            return Ok(());
        }
        let compression = options.compression;
        let reader = RdfReader::new(options).with_blank_node_scope(self.document_scope(&resource));
        let loaded = match self.source_stream(&resource, compression, fetched) {
            Some(source) => reader
                .read(source, self.dictionary, self.context)
                .map_err(|diagnostic| diagnostic.with_resource(resource.clone())),
            None => reader.read_path(
                self.resource_path(import.span, &resource),
                self.dictionary,
                self.context,
            ),
        }
        .map_err(ImportFailure::Reading)?;
        self.absorb(table, layout.delivered(), loaded, &resource);
        Ok(())
    }
}
