//! Import directives: the collected imports, executed once every predicate's arity is known,
//! and what every format shares.
//!
//! Every format shares:
//! - the checks an import passes before its format's own,
//! - opening the bytes registered or fetched under a resource name,
//! - registering the table an import loads into and seeding its column types,
//! - reporting the rows a load dropped.
//!
//! Each format has its own submodule.
//! [prefetch] fetches the remote resources before any import reads.
//! The directive options are evaluated in [options](super::options).

mod dsv;
mod json;
mod parquet;
mod prefetch;
mod rdf;
mod sparql;

use std::{
    collections::hash_map::Entry,
    path::{Path, PathBuf},
    sync::Arc,
};

use nemo_io::{
    compression::Compression, file_format::FileFormat, load::Loaded, row_filter::RowFilter,
    selection::RowSelection,
};
use nemo_physical::{
    diagnostics::Diagnostic, function::Outcome, management::TableId, trie::RowBuffer, value::Type,
};

use crate::{
    diagnostics::{AboutPredicate, AboutResource, TranslationError, TypeError},
    engine::{
        compile::layout::ColumnLayout,
        compile::optimize::{column_variable, conjoin},
        compile::translation::Translator,
        link::directory_of,
    },
    model::{Atom, Operation, Span, Substitution, Term, Variable, VariableKind, Walk},
    pipeline::Pipeline,
    values::TypeSyntax,
};

/// One import awaiting execution.
pub(super) struct PendingImport {
    /// The receiving predicate, resolved.
    pub(super) predicate: String,
    /// The column variables named after the predicate, if any.
    pub(super) variables: Vec<String>,
    /// The format.
    pub(super) format: FileFormat,
    /// The format options.
    pub(super) options: Vec<(Term, Term)>,
    /// The guard bindings.
    pub(super) guards: Vec<(String, Term)>,
    /// The binding atoms of the `where` clause, empty for an import without one.
    pub(super) bindings: Vec<Atom>,
    /// The conditions of the `where` clause, empty for an import without one.
    pub(super) conditions: Vec<Operation>,
    /// The directive's source position.
    pub(super) span: Option<Span>,
    /// Whether the import's data is already stored: its options are validated
    /// and the predicate registers, but nothing is read.
    pub(super) preloaded: bool,
    /// The rule index reserved for a bound import's fetch rule.
    #[cfg_attr(not(feature = "sparql"), allow(dead_code))]
    pub(super) rule_index: usize,
}

/// The rows a reader dropped from one resource as malformed, reported as a warning.
pub(crate) struct DroppedRows<'a> {
    /// The resource read.
    pub(crate) resource: &'a str,
    /// Rows that did not fit the format.
    pub(crate) malformed: u64,
    /// The line of the first malformed row, if the reader knows it.
    pub(crate) first_malformed: Option<u64>,
}

impl DroppedRows<'_> {
    /// Reports the drops to the pipeline, or nothing if nothing was dropped.
    pub(crate) fn report(&self, pipeline: &mut Pipeline) {
        if self.malformed == 0 {
            return;
        }
        let mut warning = TranslationError::MalformedRows {
            count: self.malformed.into(),
            resource: self.resource.to_owned(),
        }
        .diagnostic()
        .with_resource(self.resource.to_owned());
        if let Some(line) = self.first_malformed {
            warning = warning.with_note(format!("the first one is on line {line}"));
        }
        pipeline.report(warning);
    }
}

/// Why an import failed: its directive, which a translation reading no data finds as well,
/// or reading its data.
pub(super) enum ImportFailure {
    /// The directive is at fault: its options, guards, columns, or declaration.
    Directive(Diagnostic),
    /// Reading the data failed.
    Reading(Diagnostic),
}

impl From<Diagnostic> for ImportFailure {
    /// Takes a problem as the directive's, which it is unless reading found it.
    fn from(problem: Diagnostic) -> Self {
        ImportFailure::Directive(problem)
    }
}

/// Returns the resource an import names, which every format but SPARQL needs.
fn required_resource(
    resource: Option<String>,
    import: &PendingImport,
) -> Result<String, Diagnostic> {
    resource.ok_or_else(|| {
        TranslationError::OptionMissing {
            format: import.format.name().to_owned(),
            option: String::from("resource"),
        }
        .at(import.span)
    })
}

impl<'a> Translator<'a> {
    /// Returns the path a relative resource names, for a directive at the given span.
    ///
    /// The resource joins the engine's import directory if one is set,
    /// and the directory of the file the directive was written in otherwise,
    /// or the working directory for a directive without a file.
    pub(super) fn resource_path(&self, span: Option<Span>, resource: &str) -> PathBuf {
        let directory = match self.import_directory {
            Some(directory) => directory.to_path_buf(),
            None => span
                .and_then(|span| self.pipeline.file_name(span.file))
                .map_or_else(|| PathBuf::from("."), |name| directory_of(Path::new(name))),
        };
        // A resource in the working directory keeps the name it was written with,
        // which is how diagnostics name it.
        if directory.as_path() == Path::new(".") {
            return PathBuf::from(resource);
        }
        directory.join(resource)
    }

    /// Executes the collected imports.
    ///
    /// Rows load through the declared column formats (`term` by default)
    /// and join the predicate's facts.
    /// Malformed rows are skipped, counted, and reported.
    /// DSV imports resolve their options in directive order, then load as one batch.
    /// Other formats load one by one.
    pub(super) fn execute_imports(&mut self) -> Result<(), Diagnostic> {
        let mut imports = std::mem::take(&mut self.imports);
        // Remote resources are fetched first, all together, or the
        // translation suspends for them before anything reads.
        let fetched = self.prefetch(&mut imports)?;
        let mut batch = Vec::new();
        for (import, fetched) in imports.iter().zip(fetched) {
            let outcome = self
                .admissible(import)
                .map_err(ImportFailure::from)
                .and_then(|()| match import.format {
                    FileFormat::Rdf(_) => self.rdf_import(import, fetched),
                    FileFormat::Json => self.json_import(import, fetched),
                    FileFormat::Sparql => self.sparql_import(import, fetched),
                    FileFormat::Parquet => self.parquet_import(import),
                    FileFormat::Csv | FileFormat::Tsv | FileFormat::Dsv => self
                        .prepare_dsv(import, fetched)
                        .map(|prepared| batch.extend(prepared)),
                });
            match outcome {
                Ok(()) => {}
                Err(ImportFailure::Directive(problem)) => self.recover(problem)?,
                Err(ImportFailure::Reading(problem)) => return Err(problem),
            }
        }
        // A translation reading no data validates the options above.
        if !self.purpose.reads() {
            return Ok(());
        }
        self.load_dsv_batch(batch)
    }

    /// Runs the checks every import passes before its format's own.
    ///
    /// - An `@input` predicate is bound by a `@use` of the program,
    ///   so importing into it is an error.
    /// - Column variables and binding atoms refer to a SPARQL query's projection,
    ///   which no other format has.
    fn admissible(&self, import: &PendingImport) -> Result<(), Diagnostic> {
        if self.inputs.contains(&import.predicate) {
            return Err(TranslationError::ImportIntoInput {
                predicate: import.predicate.clone(),
            }
            .at(import.span));
        }
        if import.format != FileFormat::Sparql && !import.bindings.is_empty() {
            return Err(TranslationError::BindingsOnFormat {
                format: import.format.to_string(),
            }
            .at(import.bindings[0].span.or(import.span)));
        }
        Ok(())
    }

    /// Returns the registered bytes behind a resource name, opened as a stream
    /// (gzip unwrapped by name),
    /// or `None` if the resource is not registered and reads from the filesystem instead.
    fn registered_stream(
        &self,
        resource: &str,
        compression: Compression,
    ) -> Option<Box<dyn std::io::Read>> {
        self.resources
            .get(resource)
            .map(|bytes| compression.open_bytes(bytes.clone(), resource))
    }

    /// Returns the bytes an import reads if they are not on the filesystem,
    /// registered under the resource name or fetched, opened as a stream,
    /// or `None` for a file.
    fn source_stream(
        &self,
        resource: &str,
        compression: Compression,
        fetched: Option<Arc<[u8]>>,
    ) -> Option<Box<dyn std::io::Read>> {
        self.registered_stream(resource, compression)
            .or_else(|| fetched.map(|bytes| compression.open_bytes(bytes, resource)))
    }

    /// Registers the table an import loads into,
    /// given the types the reader produces per kept column,
    /// and seeds its column types with those of the delivered columns,
    /// narrowed to the rows the conditions of its `where` clause keep.
    fn register_import(
        &mut self,
        import: &PendingImport,
        kept_types: Vec<Type>,
        layout: &ColumnLayout,
    ) -> Result<TableId, Diagnostic> {
        let table = self.table_named(import.predicate.clone(), layout.delivered(), import.span)?;
        let delivered =
            self.narrowed_by_conditions(import, layout, layout.deliver(kept_types.clone()))?;
        // The declaration covers every kept column, the pruned ones included,
        // so that pruning never changes what is checked.
        let checked: Vec<Type> = if kept_types.len() == layout.kept() {
            (0..layout.kept())
                .map(|kept| match layout.delivered_position(kept) {
                    Some(position) => delivered[position].clone(),
                    None => kept_types[kept].clone(),
                })
                .collect()
        } else {
            delivered.clone()
        };
        self.seed_import(import, table, &checked, delivered)?;
        Ok(table)
    }

    /// Returns the types of an import's delivered columns
    /// narrowed to the rows the conditions of its `where` clause keep.
    fn narrowed_by_conditions(
        &self,
        import: &PendingImport,
        layout: &ColumnLayout,
        mut column_types: Vec<Type>,
    ) -> Result<Vec<Type>, Diagnostic> {
        let Some(condition) = self.own_condition(import, layout)? else {
            return Ok(column_types);
        };
        let (inputs, program) = self.condition(&condition)?;
        let read: Vec<Type> = inputs
            .iter()
            .map(|&column| column_types[column].clone())
            .collect();
        let narrowed = program.narrow(&read, &self.label_types, &Outcome::Holds);
        for (&column, narrowed) in inputs.iter().zip(narrowed) {
            column_types[column] = column_types[column].intersect(&narrowed);
        }
        Ok(column_types)
    }

    /// Returns the conditions of an import's `where` clause, conjoined,
    /// over the `column N` pseudo-variables of the delivered columns,
    /// or `None` if it has none.
    ///
    /// The import's column variables name its source columns, one each,
    /// and a condition may read only those the import delivers.
    fn own_condition(
        &self,
        import: &PendingImport,
        layout: &ColumnLayout,
    ) -> Result<Option<Operation>, Diagnostic> {
        let named = !import.variables.is_empty();
        if (named || !import.conditions.is_empty())
            && import.variables.len() != layout.source_width()
        {
            return Err(TranslationError::ImportColumnCount {
                declared: import.variables.len().into(),
                arity: layout.source_width().into(),
            }
            .at(import.span)
            .with_hint("name every column after the predicate: `@import q(?x, ?y) from …`"));
        }
        for (position, name) in import.variables.iter().enumerate() {
            if import.variables[..position].contains(name) {
                return Err(TranslationError::DuplicateColumnVariable {
                    variable: name.clone(),
                }
                .at(import.span));
            }
        }
        if import.conditions.is_empty() {
            return Ok(None);
        }
        let mut columns: Vec<(Variable, Term)> = Vec::new();
        for (source, name) in import.variables.iter().enumerate() {
            if let Some(delivered) = layout.delivered_source(source) {
                columns.push((Variable::universal(name), column_variable(delivered)));
            }
        }
        let substitution: Substitution = columns.into_iter().collect();
        let mut conditions = Vec::with_capacity(import.conditions.len());
        for condition in &import.conditions {
            for variable in condition.variables() {
                if variable.kind != VariableKind::Universal {
                    continue;
                }
                let span = condition.span.or(import.span);
                match import
                    .variables
                    .iter()
                    .position(|name| *name == variable.name)
                {
                    None => {
                        return Err(TranslationError::BindingVariableUnknown {
                            variable: variable.name.clone(),
                        }
                        .at(span));
                    }
                    Some(source) if layout.delivered_source(source).is_none() => {
                        assert!(
                            layout.skips(source),
                            "the projection pass keeps the columns an import's conditions read"
                        );
                        return Err(TranslationError::ConditionOnSkippedColumn {
                            variable: variable.name.clone(),
                        }
                        .at(span));
                    }
                    Some(_) => {}
                }
            }
            let mut condition = condition.clone();
            substitution.apply(&mut condition);
            conditions.push(condition);
        }
        Ok(conjoin(conditions))
    }

    /// Returns the condition an import's rows must pass, over its delivered columns:
    /// the conditions of its `where` clause and the condition the import plan pushed for its
    /// predicate, conjoined, as the operation it compiles from and as a row filter;
    /// `None` if there is neither.
    pub(super) fn import_condition(
        &self,
        import: &PendingImport,
        layout: &ColumnLayout,
    ) -> Result<Option<(Operation, RowFilter)>, Diagnostic> {
        let own = self.own_condition(import, layout)?;
        let pushed = self.plan.condition(&import.predicate).cloned();
        let Some(condition) = conjoin(own.into_iter().chain(pushed).collect()) else {
            return Ok(None);
        };
        let (inputs, program) = self.condition(&condition)?;
        Ok(Some((condition, RowFilter::new(inputs, program))))
    }

    /// Returns what a reader delivers of an import under a layout:
    /// the delivered columns, and the rows that pass the [import_condition](Self::import_condition).
    pub(super) fn import_selection(
        &self,
        import: &PendingImport,
        layout: &ColumnLayout,
    ) -> Result<RowSelection, Diagnostic> {
        let mut selection = self.plan.selection(&import.predicate, layout);
        selection.filter = self
            .import_condition(import, layout)?
            .map(|(_, filter)| filter);
        Ok(selection)
    }

    /// Records what an import can write into a table's columns, and checks it against a
    /// declaration.
    ///
    /// The formats are the type of what an import reads, and no row is checked as it loads,
    /// so a column whose format reads a value its declared type does not contain is an error.
    /// `checked` holds a type per kept column, `column_types` one per delivered column.
    fn seed_import(
        &mut self,
        import: &PendingImport,
        table: TableId,
        checked: &[Type],
        column_types: Vec<Type>,
    ) -> Result<(), Diagnostic> {
        let predicate = import.predicate.as_str();
        if let Some(declaration) = self.declarations.get(predicate) {
            // The declaration and the checked types are indexed by the kept columns.
            for (column, (column_type, declared)) in
                checked.iter().zip(&declaration.types).enumerate()
            {
                if !column_type.is_subset(declared, &self.label_types) {
                    let kind = TypeSyntax::of(column_type).to_string();
                    // Only a delimited format reads a column as a chosen type.
                    let hint = if import.format.is_dsv() {
                        String::from(
                            "read the column in the format named like its declared type, or \
                             import into an undeclared predicate and narrow its values with a rule",
                        )
                    } else {
                        format!(
                            "declare the column as {kind}, or import into an undeclared \
                             predicate and narrow its values with a rule"
                        )
                    };
                    return Err(TypeError::ImportOutsideDeclaration {
                        kind,
                        predicate: predicate.to_owned(),
                        column: column + 1,
                        declared: TypeSyntax::of(declared).to_string(),
                    }
                    .at(import.span)
                    .with_hint(hint)
                    .with_predicate(predicate.to_owned()));
                }
            }
        }
        let seeds = self
            .seeds
            .entry(table)
            .or_insert_with(|| vec![Type::EMPTY; column_types.len()]);
        for (position, column_type) in column_types.into_iter().enumerate() {
            seeds[position] = seeds[position].union(&column_type);
        }
        Ok(())
    }

    /// Takes a load's rows into a table's fact buffer, reporting the
    /// rows dropped along the way.
    fn absorb(&mut self, table: TableId, arity: usize, loaded: Loaded, resource: &str) {
        DroppedRows {
            resource,
            malformed: loaded.malformed,
            first_malformed: loaded.first_malformed,
        }
        .report(self.pipeline);
        self.errors.absorb(&loaded.errors);
        tracing::debug!(
            resource,
            rows = loaded.rows.len(),
            malformed = loaded.malformed,
            excluded = loaded.excluded,
            "import read"
        );
        if self.telemetry.enabled() {
            self.telemetry
                .record(&crate::telemetry::EngineEvent::Imported {
                    resource: resource.to_owned(),
                    rows: loaded.rows.len() as u64,
                });
        }

        // The first rows a table receives become its fact buffer as they
        // are; later ones join it.
        match self.facts.entry(table) {
            Entry::Vacant(entry) if loaded.rows.arity() == arity => {
                entry.insert(loaded.rows);
            }
            entry => {
                let buffer = entry.or_insert_with(|| RowBuffer::new(arity));
                for index in 0..loaded.rows.len() {
                    buffer.push_row(loaded.rows.row(index));
                }
            }
        }
    }
}
