//! JSON imports: resolving a directive into the formats and keys of its columns,
//! and loading the records of the resource as the rows of a predicate.

use std::sync::Arc;

use nemo_io::{
    compression::Compression,
    file_format::FileFormat,
    json::{JsonReader, JsonReaderOptions},
    load::Reader,
    value_format::{ColumnFormat, ValueFormat},
};
use nemo_physical::value::Type;

use crate::{
    diagnostics::AboutResource,
    engine::{
        compile::translation::{
            Translator,
            imports::{ImportFailure, PendingImport, required_resource},
            options::PlainDirectiveOptions,
        },
        registered::Registered,
    },
};

impl<'a> Translator<'a> {
    /// Executes one JSON import (see [execute_imports](Self::execute_imports)).
    ///
    /// The resource is a document that holds an array of records,
    /// or has one record per line for the format `jsonl`.
    pub(super) fn json_import(
        &mut self,
        import: &PendingImport,
        fetched: Option<Arc<[u8]>>,
    ) -> Result<(), ImportFailure> {
        let bindings = self.guard_bindings(&import.guards)?;
        let directive = self.plain_directive(import.format, &import.options, &bindings, true)?;
        let resource = required_resource(directive.resource.clone(), import)?;
        let lines = import.format == FileFormat::Jsonl;
        let Some(formats) =
            self.json_formats(import, &directive, &resource, lines, fetched.as_ref())?
        else {
            return Ok(());
        };

        // The directive's formats keep or skip the columns of the records,
        // the plan prunes some of the kept ones, and the reader delivers only the rest.
        let kept = formats
            .iter()
            .map(|format| format.value != ValueFormat::Skip)
            .collect();
        let layout = self.plan.layout(&import.predicate, kept);

        // What the kept columns can hold follows from their formats alone.
        let kept_types: Vec<Type> = formats
            .iter()
            .filter(|format| format.value != ValueFormat::Skip)
            .map(|format| Type::of_domains(format.domains()))
            .collect();
        let mut options = JsonReaderOptions::new(formats);
        options.lines = lines;
        options.columns = directive.columns;
        options.path = directive.path;
        options.limit = directive.limit;
        options.compression = directive.compression;
        options.selection = self.import_selection(import, &layout)?;
        let table = self.register_import(import, kept_types, &layout)?;
        if self.options_only(import) {
            return Ok(());
        }

        let compression = options.compression;
        let reader = JsonReader::new(options);
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
        self.absorb(table, layout.delivered(), loaded, &resource, true);

        Ok(())
    }

    /// Returns the column formats of a JSON import, one per column of its records,
    /// or `None` if nothing fixes the number of columns.
    ///
    /// - The directive's `format` option fixes them.
    /// - Otherwise the `columns` option, the directive's column variables, a `@declare` or the
    ///   predicate's registration fix the number of columns, and every column is read as
    ///   `term`.
    /// - Otherwise the first record of the resource gives the number of columns,
    ///   which a translation that reads no data does not look at.
    ///
    /// A `@declare` chooses no format: it is checked against the types that the formats give.
    fn json_formats(
        &mut self,
        import: &PendingImport,
        directive: &PlainDirectiveOptions,
        resource: &str,
        lines: bool,
        fetched: Option<&Arc<[u8]>>,
    ) -> Result<Option<Vec<ColumnFormat>>, ImportFailure> {
        let terms = |columns: usize| vec![ColumnFormat::new(ValueFormat::Term); columns];

        if let Some(formats) = &directive.formats {
            return Ok(Some(formats.clone()));
        }
        if let Some(names) = &directive.columns {
            return Ok(Some(terms(names.len())));
        }
        if !import.variables.is_empty() {
            return Ok(Some(terms(import.variables.len())));
        }
        if let Some(declaration) = self.declarations.get(&import.predicate) {
            return Ok(Some(terms(declaration.types.len())));
        }
        if let Some(&Registered { arity, .. }) = self.predicates.get(&import.predicate) {
            // The table of a pruned predicate has only the delivered columns,
            // but a record still has every kept column.
            let kept = self.plan.stored_layout(&import.predicate, arity).kept();

            return Ok(Some(terms(kept)));
        }
        if !self.purpose.reads() {
            self.open.insert(import.predicate.clone());
            return Ok(None);
        }

        let pointer = directive.path.as_deref();
        let sniffed = match self.source_stream(resource, Compression::Auto, fetched.cloned()) {
            Some(source) => JsonReader::sniff_width_from(source, lines, pointer)
                .map_err(|diagnostic| diagnostic.with_resource(resource.to_owned())),
            None => JsonReader::sniff_width(
                self.resource_path(import.span, resource),
                lines,
                pointer,
                directive.compression,
            ),
        }
        .map_err(ImportFailure::Reading)?;

        match sniffed {
            Some(width) => Ok(Some(terms(width))),
            // A resource without a record, or whose first record is no array,
            // gives no number of columns, so there is nothing to register.
            None => {
                self.open.insert(import.predicate.clone());
                Ok(None)
            }
        }
    }
}
