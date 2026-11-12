//! Parquet imports: loading a file's columns into a predicate with as many columns.
//!
//! This module is part of a build with the `parquet` feature.

use std::path::Path;

use nemo_io::{parquet::ParquetReader, remote::Request};
use nemo_physical::value::Type;

use crate::{
    diagnostics::TranslationError,
    engine::{
        compile::translation::{
            Translator,
            imports::{ImportFailure, PendingImport, required_resource},
            unsupported,
        },
        stores::Registered,
    },
};

impl<'a> Translator<'a> {
    /// Executes one Parquet import (see [execute_imports](Self::execute_imports)).
    pub(super) fn parquet_import(&mut self, import: &PendingImport) -> Result<(), ImportFailure> {
        let bindings = self.guard_bindings(&import.guards)?;
        let directive = self.plain_directive(import.format, &import.options, &bindings, true)?;
        let resource = required_resource(directive.resource, import)?;
        if Request::is_remote(&resource)
            || self.setup.resources.contains_key(&resource)
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

        let layout = self.state.plan.layout_of(&import.predicate, width);
        let selection = self.import_selection(import, &layout)?;
        let table = self.register_import(import, vec![Type::ANY; layout.kept()], &layout)?;
        if self.options_only(import) {
            return Ok(());
        }

        let file_arity = ParquetReader::width(&path).map_err(ImportFailure::Reading)?;

        // The import names as many columns as the width,
        // so the file disagrees with the names.
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
            .read_path(&path, self.stores.dictionary, self.setup.context)
            .map_err(ImportFailure::Reading)?;
        self.absorb(table, layout.delivered(), loaded, &resource, false);
        Ok(())
    }

    /// Returns the number of columns of a Parquet import, all of them kept,
    /// or `None` if a translation that reads no data leaves it open.
    ///
    /// A registered predicate, a declaration or the directive's column names fix it.
    /// Otherwise the file's schema does, which is read from the footer without the data.
    /// A translation that reads no data does not read the schema,
    /// and marks the predicate open instead.
    fn parquet_width(
        &mut self,
        import: &PendingImport,
        path: &Path,
    ) -> Result<Option<usize>, ImportFailure> {
        Ok(Some(match self.stores.predicates.get(&import.predicate) {
            Some(&Registered { arity, .. }) => self
                .state
                .plan
                .stored_layout(&import.predicate, arity)
                .kept(),
            None => match self.state.declarations.get(&import.predicate) {
                Some(declaration) => declaration.types.len(),
                None if !import.variables.is_empty() => import.variables.len(),
                None if !self.setup.purpose.reads() => {
                    self.state.open.insert(import.predicate.clone());
                    return Ok(None);
                }
                None => ParquetReader::width(path).map_err(ImportFailure::Reading)?,
            },
        }))
    }
}
