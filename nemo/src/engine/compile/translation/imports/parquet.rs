//! Parquet imports: a file's columns into a predicate of its width.

use crate::{
    diagnostics::TranslationError,
    engine::compile::translation::{
        Translator,
        imports::{ImportFailure, PendingImport},
    },
};

#[cfg(feature = "parquet")]
impl<'a> Translator<'a> {
    /// Executes one Parquet import (see [execute_imports](Self::execute_imports)).
    pub(super) fn parquet_import(&mut self, import: &PendingImport) -> Result<(), ImportFailure> {
        use nemo_io::{parquet::ParquetReader, remote};
        use nemo_physical::value::Type;

        use crate::engine::compile::translation::{imports::required_resource, unsupported};

        let bindings = self.guard_bindings(&import.guards)?;
        let directive = self.plain_directive(import.format, &import.options, &bindings, true)?;
        let resource = required_resource(directive.resource, import)?;
        if remote::Request::is_remote(&resource)
            || self.resources.contains_key(&resource)
            || resource.is_empty()
        {
            // The Parquet reader needs a seekable file.
            return Err(unsupported(
                "a remote, in-memory, or standard-input Parquet resource",
                import.span,
            )
            .into());
        }
        let path = self.resource_path(import.span, &resource);
        let Some(width) = self.parquet_width(import, &path)? else {
            return Ok(());
        };
        let layout = self.plan.layout_of(&import.predicate, width);
        let selection = self.import_selection(import, &layout)?;
        let table = self.register_import(import, vec![Type::ANY; layout.kept()], &layout)?;
        if self.options_only(import) {
            return Ok(());
        }
        let file_arity = ParquetReader::arity(&path).map_err(ImportFailure::Reading)?;
        // Named column variables are as many as the width, so the file
        // disagrees with them.
        if file_arity != width && !import.variables.is_empty() {
            return Err(ImportFailure::Reading(
                TranslationError::ImportColumnCount {
                    declared: import.variables.len().into(),
                    arity: file_arity.into(),
                }
                .at(import.span)
                .with_hint("name every column after the predicate: `@import q(?x, ?y) from …`"),
            ));
        }
        if file_arity != width {
            return Err(ImportFailure::Reading(
                TranslationError::ParquetArity {
                    resource,
                    columns: file_arity.into(),
                    predicate: import.predicate.clone(),
                    arity: width.into(),
                }
                .at(import.span),
            ));
        }
        let loaded = ParquetReader::with_selection(selection)
            .read_path(&path, self.dictionary, self.context)
            .map_err(ImportFailure::Reading)?;
        self.absorb(table, layout.delivered(), loaded, &resource);
        Ok(())
    }

    /// Returns the width of a Parquet import, every file column kept, or `None` if a check
    /// leaves it open.
    ///
    /// A registered predicate or a declaration fixes it.
    /// Otherwise the file's schema does, read from the footer without the data.
    /// A translation reading no data does not read the schema, and marks the predicate open
    /// instead.
    fn parquet_width(
        &mut self,
        import: &PendingImport,
        path: &std::path::Path,
    ) -> Result<Option<usize>, ImportFailure> {
        use nemo_io::parquet::ParquetReader;

        Ok(Some(match self.predicates.get(&import.predicate) {
            Some(&crate::engine::registered::Registered { arity, .. }) => {
                self.plan.stored_layout(&import.predicate, arity).kept()
            }
            None => match self.declarations.get(&import.predicate) {
                Some(declaration) => declaration.types.len(),
                None if !import.variables.is_empty() => import.variables.len(),
                None if !self.purpose.reads() => {
                    self.open.insert(import.predicate.clone());
                    return Ok(None);
                }
                None => ParquetReader::arity(path).map_err(ImportFailure::Reading)?,
            },
        }))
    }
}

#[cfg(not(feature = "parquet"))]
impl<'a> Translator<'a> {
    /// Returns an error for a Parquet import in a run of a build without the `parquet` feature,
    /// rather than misreading it.
    ///
    /// A check accepts what this build cannot validate,
    /// since the program may be meant for a build that can.
    pub(super) fn parquet_import(&mut self, import: &PendingImport) -> Result<(), ImportFailure> {
        use crate::engine::compile::translation::Purpose;

        if self.purpose == Purpose::Check {
            self.open.insert(import.predicate.clone());
            return Ok(());
        }
        Err(TranslationError::FeatureUnavailable {
            what: String::from("a Parquet import"),
            feature: String::from("parquet"),
        }
        .at(import.span)
        .into())
    }
}
