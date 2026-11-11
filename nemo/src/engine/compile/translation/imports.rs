//! Import directives: executing the collected imports once every predicate's arity is known.
//!
//! Each format has its own submodule, and this module has what they share:
//! - the checks that an import passes before its format's own;
//! - opening the bytes registered or fetched under a resource name;
//! - registering the table that an import loads into, and recording its column types;
//! - reporting the rows that a load dropped.
//!
//! [prefetch] fetches the remote resources before the imports read their data,
//! and [options](super::options) evaluates the directive options.

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
use nemo_physical::{diagnostics::Diagnostic, function::Outcome, management::TableId, value::Type};

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

/// One import directive awaiting execution.
pub(super) struct PendingImport {
    /// The resolved predicate that receives the rows.
    pub(super) predicate: String,
    /// The names of the predicate's columns, if the directive gives them (`q(?x, ?y)`).
    pub(super) variables: Vec<String>,
    /// The format.
    pub(super) format: FileFormat,
    /// The format options.
    pub(super) options: Vec<(Term, Term)>,
    /// The guard bindings.
    pub(super) guards: Vec<(String, Term)>,
    /// The atoms of the `where` clause, or empty for an import without them.
    pub(super) bindings: Vec<Atom>,
    /// The conditions of the `where` clause, or empty for an import without them.
    pub(super) conditions: Vec<Operation>,
    /// The directive's source position.
    pub(super) span: Option<Span>,
    /// The position of the directive among the translated statements,
    /// by which its remote resource is fetched, if it reads one.
    pub(super) statement: usize,
    /// Whether the import's data is already stored.
    ///
    /// Its options are then validated and its predicate is registered, but nothing is read.
    pub(super) preloaded: bool,
    /// The rule index reserved for the fetch rule of an import with atoms in its `where`
    /// clause.
    pub(super) rule_index: usize,
}

/// The malformed rows that a reader dropped from one resource,
/// which are reported as a warning.
pub(crate) struct DroppedRows<'a> {
    /// The name of the resource.
    pub(crate) resource: &'a str,
    /// The number of malformed rows.
    pub(crate) malformed: u64,
    /// The line of the first malformed row, if the reader knows it.
    pub(crate) first_malformed: Option<u64>,
}

impl DroppedRows<'_> {
    /// Reports the dropped rows to the pipeline as a warning, if there are any.
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

/// Why an import failed: because of its directive, or while reading its data.
///
/// A translation that reads no data also finds the problems of the directive.
pub(super) enum ImportFailure {
    /// The directive is at fault: its options, guards, columns, or declaration.
    Directive(Diagnostic),
    /// Reading the data failed.
    Reading(Diagnostic),
}

impl From<Diagnostic> for ImportFailure {
    /// Treats a problem as the directive's, which it is unless it was found while reading.
    fn from(problem: Diagnostic) -> Self {
        ImportFailure::Directive(problem)
    }
}

/// Returns the resource that an import names, or fails if it names none.
///
/// Every format except SPARQL needs a resource.
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
    /// Returns the path that a relative resource names, for a directive at `span`.
    ///
    /// The resource is relative to the engine's import directory if one is set.
    /// Otherwise it is relative to the directory of the file that contains the directive,
    /// or to the working directory for a directive without a file.
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
    /// The rows are read in the formats of their columns, and are added to the predicate's
    /// facts.
    /// Malformed rows are skipped, counted and reported.
    /// The DSV imports resolve their options in directive order, and are then loaded as one
    /// batch.
    /// The other formats are loaded one by one.
    pub(super) fn execute_imports(&mut self) -> Result<(), Diagnostic> {
        let mut imports = std::mem::take(&mut self.imports);
        // The remote resources were fetched before the translation was committed,
        // and their bytes are taken out before any import reads.
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
        // A translation that reads no data only validates the options, as the loop did.
        if !self.purpose.reads() {
            return Ok(());
        }
        self.load_dsv_batch(batch)
    }

    /// Runs the checks that every import passes before its format's own.
    ///
    /// - A `@use` of the program binds an `@input` predicate,
    ///   so importing into it is an error.
    /// - The atoms of a `where` clause bind the variables of a SPARQL query's projection,
    ///   so they are an error in an import of any other format.
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

    /// Returns the bytes registered under a resource name, opened as a stream,
    /// or `None` if no bytes are registered under it.
    ///
    /// The stream is decompressed according to `compression` and the name.
    fn registered_stream(
        &self,
        resource: &str,
        compression: Compression,
    ) -> Option<Box<dyn std::io::Read>> {
        self.resources
            .get(resource)
            .map(|bytes| compression.open_bytes(bytes.clone(), resource))
    }

    /// Returns the bytes that an import reads, opened as a stream,
    /// if they are registered under the resource name or were `fetched`,
    /// and `None` for a file.
    fn source_stream(
        &self,
        resource: &str,
        compression: Compression,
        fetched: Option<Arc<[u8]>>,
    ) -> Option<Box<dyn std::io::Read>> {
        self.registered_stream(resource, compression)
            .or_else(|| fetched.map(|bytes| compression.open_bytes(bytes, resource)))
    }

    /// Registers the table that an import loads into, and records the types of its delivered
    /// columns.
    ///
    /// `kept_types` contains the type that the reader produces for each kept column.
    /// The recorded types are narrowed to the rows that the conditions of the import's `where`
    /// clause keep, and the types are checked against the predicate's declaration.
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

    /// Returns `column_types`, the types of an import's delivered columns,
    /// narrowed to the rows that the conditions of its `where` clause keep.
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

    /// Returns the conjunction of the conditions of an import's `where` clause,
    /// or `None` if it has none.
    ///
    /// The import's variables name its source columns, one each,
    /// and a condition may read only the delivered ones.
    /// In the returned condition, delivered column `N` is the variable `column N`.
    /// Fails if the import names another number of columns than its source has,
    /// or names a column twice, or if a condition reads an unknown variable or a skipped
    /// column.
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

    /// Returns the condition that an import's rows must satisfy, over its delivered columns,
    /// or `None` if there is none.
    ///
    /// It is the conjunction of the conditions of the import's `where` clause
    /// and the condition that the import plan has for its predicate.
    /// It is returned both as written and compiled to a row filter.
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

    /// Returns the selection of an import under a layout: its delivered columns,
    /// and the rows that satisfy its [import_condition](Self::import_condition).
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

    /// Records the types that an import can write to a table's columns,
    /// and checks them against the predicate's declaration.
    ///
    /// The formats give the type of what an import reads, and no row is checked as it is
    /// loaded.
    /// So a column whose format can produce a value outside its declared type is an error.
    /// `checked` contains a type per kept column, and `column_types` one per delivered column.
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
                    // Only a DSV format can read a column in a chosen format.
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
        self.seed(table, &column_types);
        Ok(())
    }

    /// Adds a load's rows to a table's fact buffer, and reports the rows that the load dropped.
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

        debug_assert_eq!(
            loaded.rows.arity(),
            arity,
            "a load has as many columns as its table"
        );

        // The first rows that a table receives become its fact buffer as they are,
        // and later rows are added to it.
        match self.facts.entry(table) {
            Entry::Vacant(entry) => {
                entry.insert(loaded.rows);
            }
            Entry::Occupied(mut entry) => {
                let buffer = entry.get_mut();
                for index in 0..loaded.rows.len() {
                    buffer.push_row(loaded.rows.row(index));
                }
            }
        }
    }
}
