//! DSV imports: resolving a directive into its options, table and column formats,
//! and loading the DSV imports of one translation as one batch.

use std::sync::Arc;

use nemo_io::{
    dsv::DsvOptions,
    load::{DsvResource, Resource, ResourceBatch},
    value_format::{ColumnFormat, ValueFormat},
};
use nemo_physical::{diagnostics::Diagnostic, management::TableId, value::Type};

use crate::{
    diagnostics::AboutResource,
    engine::{
        compile::translation::{
            Translator,
            imports::{ImportFailure, PendingImport, required_resource},
            options::DsvDirectiveOptions,
        },
        registered::Registered,
    },
};

/// One DSV import with its options resolved, awaiting the batched load.
pub(super) struct PreparedDsv {
    /// The resource and its reading options.
    source: DsvResource,
    /// The table that receives the rows.
    table: TableId,
    /// The table's arity.
    arity: usize,
    /// The resource name, for reporting.
    resource: String,
}

/// Returns the name under which a source of registered or fetched bytes is reported:
/// the resource name, or `<stdin>` for the empty name, which stands for standard input.
fn source_name(resource: &str) -> String {
    if resource.is_empty() {
        String::from("<stdin>")
    } else {
        resource.to_owned()
    }
}

impl<'a> Translator<'a> {
    /// Resolves one DSV import for the [batched load](Self::load_dsv_batch):
    /// its evaluated options, its registered table, and its column formats.
    ///
    /// Returns `None` if nothing fixes the number of columns,
    /// such as for an empty file into a predicate without other uses,
    /// and for a preloaded import.
    pub(super) fn prepare_dsv(
        &mut self,
        import: &PendingImport,
        fetched: Option<Arc<[u8]>>,
    ) -> Result<Option<PreparedDsv>, ImportFailure> {
        let bindings = self.guard_bindings(&import.guards)?;
        let directive =
            self.dsv_directive(import.format, &import.options, &bindings, import.span, true)?;
        let resource = required_resource(directive.resource.clone(), import)?;
        let Some(formats) = self.dsv_formats(import, &directive, &resource, fetched.as_ref())?
        else {
            return Ok(None);
        };
        // The pruned columns are only checked while parsing, and not delivered
        // (see `DsvOptions::checked_only`), so that they still decide which rows are
        // well-formed.
        // The plan's positions are among the kept columns, which the directive does not skip.
        let kept = formats
            .iter()
            .map(|format| format.value != ValueFormat::Skip)
            .collect();
        let layout = self.plan.layout(&import.predicate, kept);
        let checked_only: Vec<bool> = formats
            .iter()
            .enumerate()
            .map(|(column, format)| format.value != ValueFormat::Skip && !layout.delivers(column))
            .collect();
        // What the kept columns can hold follows from their formats
        // alone, so no file is read for it.
        let kept_types: Vec<Type> = formats
            .iter()
            .filter(|format| format.value != ValueFormat::Skip)
            .map(|format| Type::of_domains(format.domains()))
            .collect();
        let table = self.register_import(import, kept_types, &layout)?;
        // A preloaded import's rows are already stored, so there is nothing to load.
        if import.preloaded {
            return Ok(None);
        }

        let mut options = DsvOptions::new(directive.delimiter, formats);
        options.checked_only = checked_only;
        options.quoting = directive.quoting;
        options.ignore_headers = directive.ignore_headers;
        options.compression = directive.compression;
        options.condition = self
            .import_condition(import, &layout)?
            .map(|(_, filter)| filter);
        options.limit = directive.limit;
        let input = match self.resources.get(&resource).cloned().or(fetched) {
            Some(bytes) => Resource::Bytes {
                name: source_name(&resource),
                bytes,
            },
            None => Resource::Path(self.resource_path(import.span, &resource)),
        };
        Ok(Some(PreparedDsv {
            source: DsvResource { input, options },
            table,
            arity: layout.delivered(),
            resource,
        }))
    }

    /// Returns the column formats of a DSV import, one per source column,
    /// or `None` if nothing fixes the number of columns.
    ///
    /// - The directive's `format` option fixes them.
    /// - Otherwise the directive's column names fix the number of columns,
    ///   and every column is read as `term`.
    /// - Otherwise a `@declare`, the predicate's registration or the file gives the number of
    ///   columns, and every column is read as `term`.
    ///
    /// A `@declare` chooses no format: it is checked against the types that the formats give.
    fn dsv_formats(
        &mut self,
        import: &PendingImport,
        directive: &DsvDirectiveOptions,
        resource: &str,
        fetched: Option<&Arc<[u8]>>,
    ) -> Result<Option<Vec<ColumnFormat>>, ImportFailure> {
        let terms = |columns: usize| vec![ColumnFormat::new(ValueFormat::Term); columns];

        if let Some(formats) = &directive.formats {
            return Ok(Some(formats.clone()));
        }
        if !import.variables.is_empty() {
            return Ok(Some(terms(import.variables.len())));
        }
        if let Some(declaration) = self.declarations.get(&import.predicate) {
            return Ok(Some(terms(declaration.types.len())));
        }

        let Some(kept) = self.dsv_width(import, directive, resource, fetched)? else {
            return Ok(None);
        };

        Ok(Some(terms(kept)))
    }

    /// Returns the number of kept columns of an import without formats or declaration,
    /// or `None` if nothing fixes it.
    ///
    /// The number comes from the predicate's registration by its other uses,
    /// or else from sniffing the file, which a translation that reads no data does not do.
    /// Without it, the predicate stays unregistered and is marked open,
    /// since its data could be any value.
    fn dsv_width(
        &mut self,
        import: &PendingImport,
        directive: &DsvDirectiveOptions,
        resource: &str,
        fetched: Option<&Arc<[u8]>>,
    ) -> Result<Option<usize>, ImportFailure> {
        let arity = match self.predicates.get(&import.predicate) {
            Some(&Registered { arity, .. }) => arity,
            None => {
                if !self.purpose.reads() {
                    self.open.insert(import.predicate.clone());
                    return Ok(None);
                }
                let sniffed =
                    match self.source_stream(resource, directive.compression, fetched.cloned()) {
                        Some(source) => DsvOptions::sniff_width_from(
                            source,
                            directive.delimiter,
                            directive.quoting,
                            directive.ignore_headers,
                        )
                        .map_err(|diagnostic| diagnostic.with_resource(resource.to_owned())),
                        None => DsvOptions::sniff_width(
                            self.resource_path(import.span, resource),
                            directive.delimiter,
                            directive.quoting,
                            directive.ignore_headers,
                            directive.compression,
                        ),
                    }
                    .map_err(ImportFailure::Reading)?;
                match sniffed {
                    Some(arity) => arity,
                    // An empty file into a predicate without other uses has no rows and no
                    // width, so there is nothing to register.
                    None => {
                        self.open.insert(import.predicate.clone());
                        return Ok(None);
                    }
                }
            }
        };
        // The table of a pruned predicate has only the delivered columns,
        // but the file still has every kept column.
        Ok(Some(
            self.plan.stored_layout(&import.predicate, arity).kept(),
        ))
    }

    /// Loads the prepared DSV imports of this translation as one batch.
    ///
    /// The rows are added to their tables' facts.
    pub(super) fn load_dsv_batch(&mut self, batch: Vec<PreparedDsv>) -> Result<(), Diagnostic> {
        if batch.is_empty() {
            return Ok(());
        }
        let sources: Vec<DsvResource> = batch
            .iter()
            .map(|prepared| prepared.source.clone())
            .collect();
        let loaded = ResourceBatch::new(sources).load(self.dictionary, self.context)?;
        for (prepared, loaded) in batch.into_iter().zip(loaded) {
            self.absorb(
                prepared.table,
                prepared.arity,
                loaded,
                &prepared.resource,
                true,
            );
        }
        Ok(())
    }
}
